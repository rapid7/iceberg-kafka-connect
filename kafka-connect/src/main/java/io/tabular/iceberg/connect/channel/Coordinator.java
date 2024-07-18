/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.channel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.*;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.kafka.clients.admin.MemberDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class Coordinator extends Channel implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String OFFSETS_SNAPSHOT_PROP_FMT = "kafka.connect.offsets.%s.%s";
    private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
    private static final String VTTS_SNAPSHOT_PROP = "kafka.connect.vtts";
    private static final Duration POLL_DURATION = Duration.ofMillis(1000);

    private static final String TXID_VALID_UNTIL_PROP = "txid_valid_until";
    private final Map<Integer, Long> lastProcessedTxIdPerPartition = new HashMap<>();
    private final Map<Integer, Long> highestTxIdPerPartition = new HashMap<>();

    private final Catalog catalog;
    private final IcebergSinkConfig config;
    private final int totalPartitionCount;
    private final String snapshotOffsetsProp;
    private final ExecutorService exec;
    private final CommitState commitState;

    public Coordinator(
            Catalog catalog,
            IcebergSinkConfig config,
            Collection<MemberDescription> members,
            KafkaClientFactory clientFactory) {
        // pass consumer group ID to which we commit low watermark offsets
        super("coordinator", config.controlGroupId() + "-coord", config, clientFactory);

        this.catalog = catalog;
        this.config = config;
        this.totalPartitionCount =
                members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
        this.snapshotOffsetsProp =
                String.format(OFFSETS_SNAPSHOT_PROP_FMT, config.controlTopic(), config.controlGroupId());
        this.exec = ThreadPools.newWorkerPool("iceberg-committer", config.commitThreads());
        this.commitState = new CommitState(config);

        // initial poll with longer duration so the consumer will initialize...
        consumeAvailable(Duration.ofMillis(1000), this::receive);
    }

    public void process() {
        if (commitState.isCommitIntervalReached()) {
            // send out begin commit
            commitState.startNewCommit();
            LOG.info("Started new commit with commit-id={}", commitState.currentCommitId().toString());

            // Iterate over each partition and its last processed txId
            lastProcessedTxIdPerPartition.forEach((partition, txId) -> {
                TxIdValidUntilEvent event = new TxIdValidUntilEvent(config.controlGroupId(), new StartCommit(commitState.currentCommitId()), partition, txId);
                send(event);
                LOG.info("Sent workers commit trigger for partition {} with commit-id={}", partition, commitState.currentCommitId().toString());
            });
        }

        consumeAvailable(POLL_DURATION, this::receive);

        if (commitState.isCommitTimedOut()) {
            commit(true);
        }
    }

    private boolean receive(Envelope envelope) {
        switch (envelope.event().type()) {
            case DATA_WRITTEN:
                TxIdValidUntilEvent dataEvent = (TxIdValidUntilEvent) envelope.event();
                highestTxIdPerPartition.merge(dataEvent.getPartition(), dataEvent.getTxId(), Math::max);
                lastProcessedTxIdPerPartition.put(dataEvent.getPartition(), dataEvent.getTxId());
                commitState.addResponse(envelope);
                return true;
            case DATA_COMPLETE:
                commitState.addReady(envelope);
                if (commitState.isCommitReady(totalPartitionCount)) {
                    commit(false);
                }
                return true;
        }
        return false;
    }

    private void commit(boolean partialCommit) {
        try {
            LOG.info("Processing commit after responses for {}, isPartialCommit {}", commitState.currentCommitId(), partialCommit);
            doCommit(partialCommit);
        } catch (Exception e) {
            LOG.warn("Commit failed, will try again next cycle", e);
        } finally {
            commitState.endCurrentCommit();
        }
    }

    private void doCommit(boolean partialCommit) {
        String txidValidUntilStr = Long.toString(calculateTxidValidUntil());

        Map<TableIdentifier, List<Envelope>> commitMap = commitState.tableCommitMap();
        String offsetsJson = offsetsJson();
        OffsetDateTime vtts = commitState.vtts(partialCommit);

        Tasks.foreach(commitMap.entrySet())
                .executeWith(exec)
                .stopOnFailure()
                .run(entry -> commitToTable(entry.getKey(), entry.getValue(), offsetsJson, vtts, txidValidUntilStr));
        // we should only get here if all tables committed successfully...
        commitConsumerOffsets();
        commitState.clearResponses();

        Event event =
                new Event(config.controlGroupId(), new CommitComplete(commitState.currentCommitId(), vtts));
        send(event);

        LOG.info(
                "Commit {} complete, committed to {} table(s), vtts {}",
                commitState.currentCommitId(),
                commitMap.size(),
                vtts);
    }

    private String offsetsJson() {
        try {
            return MAPPER.writeValueAsString(controlTopicOffsets());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void commitToTable(
            TableIdentifier tableIdentifier,
            List<Envelope> envelopeList,
            String offsetsJson,
            OffsetDateTime vtts,
            String txidValidUntilStr) {
        Table table;
        try {
            table = catalog.loadTable(tableIdentifier);
        } catch (NoSuchTableException e) {
            LOG.warn("Table not found, skipping commit: {}", tableIdentifier);
            return;
        }

        Optional<String> branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

        Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(table, branch.orElse(null));

        List<Envelope> filteredEnvelopeList =
                envelopeList.stream()
                        .filter(
                                envelope -> {
                                    Long minOffset = committedOffsets.get(envelope.partition());
                                    return minOffset == null || envelope.offset() >= minOffset;
                                })
                        .collect(toList());

        List<DataFile> dataFiles =
                Deduplicated.dataFiles(commitState.currentCommitId(), tableIdentifier, filteredEnvelopeList)
                        .stream()
                        .filter(dataFile -> dataFile.recordCount() > 0)
                        .collect(toList());

        List<DeleteFile> deleteFiles =
                Deduplicated.deleteFiles(
                                commitState.currentCommitId(), tableIdentifier, filteredEnvelopeList)
                        .stream()
                        .filter(deleteFile -> deleteFile.recordCount() > 0)
                        .collect(toList());

        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
        } else {
            if (deleteFiles.isEmpty()) {
                Transaction transaction = table.newTransaction();

                Map<Integer, List<DataFile>> filesBySpec =
                        dataFiles.stream()
                                .collect(Collectors.groupingBy(DataFile::specId, Collectors.toList()));

                List<List<DataFile>> list = Lists.newArrayList(filesBySpec.values());
                int lastIdx = list.size() - 1;
                for (int i = 0; i <= lastIdx; i++) {
                    AppendFiles appendOp = transaction.newAppend();
                    branch.ifPresent(appendOp::toBranch);

                    list.get(i).forEach(appendOp::appendFile);
                    appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
                    if (i == lastIdx) {
                        appendOp.set(snapshotOffsetsProp, offsetsJson);
                        if (vtts != null) {
                            appendOp.set(VTTS_SNAPSHOT_PROP, Long.toString(vtts.toInstant().toEpochMilli()));
                        }
                        appendOp.set(TXID_VALID_UNTIL_PROP, txidValidUntilStr);
                    }

                    appendOp.commit();
                }

                transaction.commitTransaction();
            } else {
                RowDelta deltaOp = table.newRowDelta();
                branch.ifPresent(deltaOp::toBranch);
                deltaOp.set(snapshotOffsetsProp, offsetsJson);
                deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
                if (vtts != null) {
                    deltaOp.set(VTTS_SNAPSHOT_PROP, Long.toString(vtts.toInstant().toEpochMilli()));
                }
                deltaOp.set(TXID_VALID_UNTIL_PROP, txidValidUntilStr);
                dataFiles.forEach(deltaOp::addRows);
                deleteFiles.forEach(deltaOp::addDeletes);
                deltaOp.commit();
            }

            Long snapshotId = latestSnapshot(table, branch.orElse(null)).snapshotId();
            Event event =
                    new Event(
                            config.controlGroupId(),
                            new CommitToTable(
                                    commitState.currentCommitId(),
                                    TableReference.of(config.catalogName(), tableIdentifier),
                                    snapshotId,
                                    vtts));
            send(event);

            LOG.info(
                    "Commit complete to table {}, snapshot {}, commit ID {}, vtts {}",
                    tableIdentifier,
                    snapshotId,
                    commitState.currentCommitId(),
                    vtts);
        }
    }

    // Calculate the highest txid_valid_until value across all partitions
    private long calculateTxidValidUntil() {
        // If the map is empty, return 0 as no transactions are guaranteed to be completed
        if (highestTxIdPerPartition.isEmpty()) {
            return 0L;
        }

        // Find the minimum value in the map, as it represents the highest transaction ID
        // that is guaranteed to be completed across all partitions
        long minValue = Collections.min(highestTxIdPerPartition.values());

        // Subtract 1 from the minimum value to get the last guaranteed completed transaction ID
        // If minValue is 1, then there are no completed transactions, so return 0
        return minValue > 1 ? minValue - 1 : 0;
    }

    private Snapshot latestSnapshot(Table table, String branch) {
        if (branch == null) {
            return table.currentSnapshot();
        }
        return table.snapshot(branch);
    }

    private Map<Integer, Long> lastCommittedOffsetsForTable(Table table, String branch) {
        Snapshot snapshot = latestSnapshot(table, branch);
        while (snapshot != null) {
            Map<String, String> summary = snapshot.summary();
            String value = summary.get(snapshotOffsetsProp);
            if (value != null) {
                TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
                try {
                    return MAPPER.readValue(value, typeRef);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            Long parentSnapshotId = snapshot.parentId();
            snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
        }
        return ImmutableMap.of();
    }

    @Override
    public void close() throws IOException {
        exec.shutdownNow();
        stop();
    }
}
