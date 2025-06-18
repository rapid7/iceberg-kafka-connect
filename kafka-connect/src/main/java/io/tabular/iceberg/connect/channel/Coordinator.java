/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.channel;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.events.TopicPartitionTransaction;
import io.tabular.iceberg.connect.events.TransactionDataComplete;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
// FIX: Corrected import paths
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.kafka.clients.admin.MemberDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends Channel implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String OFFSETS_SNAPSHOT_PROP_FMT = "kafka.connect.offsets.%s.%s";
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VTTS_SNAPSHOT_PROP = "kafka.connect.vtts";
  private static final Duration POLL_DURATION = Duration.ofMillis(1000);
  private static final String TXID_VALID_THROUGH_PROP = "txid-valid-through";
  private static final String TXID_MAX_PROP = "txid-max";

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final int totalPartitionCount;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;

  private final Map<Integer, Long> highestTxIdPerPartition = new ConcurrentHashMap<>();

  public Coordinator(
          Catalog catalog,
          IcebergSinkConfig config,
          Collection<MemberDescription> members,
          KafkaClientFactory clientFactory) {
    super("coordinator", config.controlGroupId() + "-coord", config, clientFactory);

    this.catalog = catalog;
    this.config = config;
    this.totalPartitionCount =
            members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
    this.snapshotOffsetsProp =
            String.format(OFFSETS_SNAPSHOT_PROP_FMT, config.controlTopic(), config.controlGroupId());
    this.exec = ThreadPools.newWorkerPool("iceberg-committer", config.commitThreads());
    this.commitState = new CommitState(config);

    consumeAvailable(Duration.ofMillis(1000), this::receive);
  }

  // FIX: Access modifier changed from private to protected to match parent class.
  @Override
  protected Map<Integer, Long> highestTxIdPerPartition() {
    return highestTxIdPerPartition;
  }

  public void process() {
    if (commitState.isCommitIntervalReached()) {
      commitState.startNewCommit();
      LOG.info("Started new commit with commit-id={}", commitState.currentCommitId().toString());
      Event event =
              new Event(config.controlGroupId(), new StartCommit(commitState.currentCommitId()));
      send(event);
      LOG.info("Sent workers commit trigger with commit-id={}", commitState.currentCommitId().toString());
    }

    consumeAvailable(POLL_DURATION, this::receive);

    if (commitState.isCommitTimedOut()) {
      commit(true);
    }
  }

  private boolean receive(Envelope envelope) {
    switch (envelope.event().type()) {
      case DATA_WRITTEN:
        commitState.addResponse(envelope);
        return true;
      case DATA_COMPLETE:
        commitState.addReady(envelope);
        if (envelope.event().payload() instanceof TransactionDataComplete) {
          TransactionDataComplete payload = (TransactionDataComplete) envelope.event().payload();
          List<TopicPartitionTransaction> txIds = payload.txIds();
          LOG.debug("Received transaction data complete event with {} txIds", txIds.size());
          txIds.forEach(
                  txId ->
                          highestTxIdPerPartition()
                                  .put(
                                          txId.partition(),
                                          compareTxIds(
                                                  highestTxIdPerPartition().getOrDefault(txId.partition(), 0L),
                                                  txId.txId())));
        }
        if (commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
    }
    return false;
  }

  private long compareTxIds(long currentTxId, long newTxId) {
    long wraparoundThreshold = 4294967296L;

    if ((newTxId > currentTxId && newTxId - currentTxId <= wraparoundThreshold / 2)
            || (newTxId < currentTxId && currentTxId - newTxId > wraparoundThreshold / 2)) {
      return newTxId;
    }

    return Math.max(currentTxId, newTxId);
  }

  private void commit(boolean partialCommit) {
    try {
      LOG.info(
              "Processing commit after responses for {}, isPartialCommit {}",
              commitState.currentCommitId(),
              partialCommit);
      doCommit(partialCommit);
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    } finally {
      // This ensures ALL state for the completed commit cycle is cleared.
      highestTxIdPerPartition().clear();
      commitState.clearResponses();
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    Map<TableIdentifier, List<Envelope>> commitMap = commitState.tableCommitMap();
    String offsetsJson = offsetsJson();
    OffsetDateTime vtts = commitState.vtts(partialCommit);

    // This logic is simple and correct because the underlying map is always clean.
    final Map<Integer, Long> currentWatermarks = highestTxIdPerPartition();

    final long finalTxIdValidThrough;
    if (currentWatermarks.size() == this.totalPartitionCount) {
      finalTxIdValidThrough = Utilities.calculateTxIdValidThrough(currentWatermarks);
    } else {
      LOG.warn(
              "Cannot calculate txid-valid-through, not all partitions have reported watermarks. Known partitions: {}/{}",
              currentWatermarks.size(),
              this.totalPartitionCount);
      finalTxIdValidThrough = -1L;
    }

    final long maxTxId = Utilities.getMaxTxId(currentWatermarks);

    Tasks.foreach(commitMap.entrySet())
            .executeWith(exec)
            .stopOnFailure()
            .run(
                    entry -> {
                      commitToTable(
                              entry.getKey(),
                              entry.getValue(),
                              offsetsJson,
                              vtts,
                              finalTxIdValidThrough,
                              maxTxId);
                    });

    commitConsumerOffsets();

    Event event =
            new Event(config.controlGroupId(), new CommitComplete(commitState.currentCommitId(), vtts));
    send(event);

    LOG.info(
            "Commit {} complete, committed to {} table(s), vtts {}, txid-valid-through {}",
            commitState.currentCommitId(),
            commitMap.size(),
            vtts,
            finalTxIdValidThrough);
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
          long txIdValidThrough,
          long maxTxId) {
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
            addTxDataToSnapshot(appendOp, txIdValidThrough, maxTxId);
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
        addTxDataToSnapshot(deltaOp, txIdValidThrough, maxTxId);
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

  private void addTxDataToSnapshot(SnapshotUpdate<?> operation, long txIdValidThrough, long maxTxId) {
    if (txIdValidThrough > -1 && maxTxId > 0) {
      operation.set(TXID_VALID_THROUGH_PROP, Long.toString(txIdValidThrough));
      operation.set(TXID_MAX_PROP, Long.toString(maxTxId));
      LOG.info("Added transaction data to snapshot: validThrough={}, max={}", txIdValidThrough, maxTxId);
    } else {
      LOG.warn("No transaction data to add to snapshot");
    }
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