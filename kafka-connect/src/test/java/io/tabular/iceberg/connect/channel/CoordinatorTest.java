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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.events.TableTopicPartitionTransaction; // ADDED
import io.tabular.iceberg.connect.events.TransactionDataComplete;
import io.tabular.iceberg.connect.fixtures.EventTestUtil;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CoordinatorTest extends ChannelTestBase {

  // The property name for max-tx-id has a typo in the original test, correcting it here
  private static final String MAX_TX_ID_PROP = "txid-max";

  @Test
  public void testCommitAppend() {
    Assertions.assertEquals(0, ImmutableList.copyOf(table.snapshots().iterator()).size());

    // CHANGED: Use the new transaction object, including the table reference
    TableReference tableRef = TableReference.of("catalog", TABLE_IDENTIFIER);
    List<TableTopicPartitionTransaction> transactionsProcessed =
            ImmutableList.of(new TableTopicPartitionTransaction("topic", 1, "catalog", TABLE_IDENTIFIER, 100L));

    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    UUID commitId =
            coordinatorTest(ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), ts, transactionsProcessed);
    table.refresh();

    assertThat(producer.history()).hasSize(3);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
            .isEqualTo(ImmutableMap.of(CTL_TOPIC_PARTITION, new OffsetAndMetadata(3L)));
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.APPEND, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    Map<String, String> summary = snapshot.summary();
    Assertions.assertEquals(commitId.toString(), summary.get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertEquals("{\"0\":3}", summary.get(OFFSETS_SNAPSHOT_PROP));
    Assertions.assertEquals(
            Long.toString(ts.toInstant().toEpochMilli()), summary.get(VTTS_SNAPSHOT_PROP));
    Assertions.assertEquals(99L, Long.valueOf(summary.get(TX_ID_VALID_THROUGH_PROP)));
    Assertions.assertEquals(100L, Long.valueOf(summary.get(MAX_TX_ID_PROP)));
  }

  @Test
  public void testCommitDelta() {
    // CHANGED: Use the new transaction object
    TableReference tableRef = TableReference.of("catalog", TABLE_IDENTIFIER);
    List<TableTopicPartitionTransaction> transactionsProcessed =
            ImmutableList.of(
                    new TableTopicPartitionTransaction("topic", 1, "catalog", TABLE_IDENTIFIER, 100L),
                    new TableTopicPartitionTransaction("topic", 2, "catalog", TABLE_IDENTIFIER, 102L),
                    new TableTopicPartitionTransaction("topic", 3, "catalog", TABLE_IDENTIFIER, 101L),
                    new TableTopicPartitionTransaction("topic", 3, "catalog", TABLE_IDENTIFIER, 102L),
                    new TableTopicPartitionTransaction("topic", 3, "catalog", TABLE_IDENTIFIER, 100L),
                    new TableTopicPartitionTransaction("topic", 3, "catalog", TABLE_IDENTIFIER, 103L),
                    new TableTopicPartitionTransaction("topic", 4, "catalog", TABLE_IDENTIFIER, 104L));

    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    UUID commitId =
            coordinatorTest(
                    ImmutableList.of(EventTestUtil.createDataFile()),
                    ImmutableList.of(EventTestUtil.createDeleteFile()),
                    ts,
                    transactionsProcessed);

    assertThat(producer.history()).hasSize(3);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
            .isEqualTo(ImmutableMap.of(CTL_TOPIC_PARTITION, new OffsetAndMetadata(3L)));
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.OVERWRITE, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    Map<String, String> summary = snapshot.summary();
    Assertions.assertEquals(commitId.toString(), summary.get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertEquals("{\"0\":3}", summary.get(OFFSETS_SNAPSHOT_PROP));
    Assertions.assertEquals(
            Long.toString(ts.toInstant().toEpochMilli()), summary.get(VTTS_SNAPSHOT_PROP));
    Assertions.assertEquals(99L, Long.valueOf(summary.get(TX_ID_VALID_THROUGH_PROP)));
    Assertions.assertEquals(104L, Long.valueOf(summary.get(MAX_TX_ID_PROP)));
  }

  @Test
  public void testCommitNoFiles() {
    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    UUID commitId = coordinatorTest(ImmutableList.of(), ImmutableList.of(), ts, ImmutableList.of());

    assertThat(producer.history()).hasSize(2);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
            .isEqualTo(ImmutableMap.of(CTL_TOPIC_PARTITION, new OffsetAndMetadata(3L)));
    assertCommitComplete(1, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(0, snapshots.size());
  }

  @Test
  public void testCommitError() {
    // this spec isn't registered with the table
    PartitionSpec badPartitionSpec =
            PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
            DataFiles.builder(badPartitionSpec)
                    .withPath(UUID.randomUUID() + ".parquet")
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(100L)
                    .withRecordCount(5)
                    .build();

    coordinatorTest(
            ImmutableList.of(badDataFile),
            ImmutableList.of(),
            OffsetDateTime.ofInstant(Instant.ofEpochMilli(0L), ZoneOffset.UTC),
            ImmutableList.of());

    // no commit messages sent
    assertThat(producer.history()).hasSize(1);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
            .isEqualTo(ImmutableMap.of());

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(0, snapshots.size());
  }

  @Test
  public void testShouldDeduplicateDataFilesBeforeAppending() {
    // CHANGED: Use the new transaction object
    TableReference tableRef = TableReference.of("catalog", TableIdentifier.of("db", "tbl"));
    List<TableTopicPartitionTransaction> transactionsProcessed =
            ImmutableList.of(new TableTopicPartitionTransaction("topic", 1, "catalog", TABLE_IDENTIFIER, 100L));

    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    DataFile dataFile = EventTestUtil.createDataFile();

    UUID commitId =
            coordinatorTest(
                    currentCommitId -> {
                      Event commitResponse =
                              new Event(
                                      config.controlGroupId(),
                                      new DataWritten(
                                              StructType.of(),
                                              currentCommitId,
                                              tableRef,
                                              ImmutableList.of(dataFile, dataFile), // duplicated data files
                                              ImmutableList.of()));

                      return ImmutableList.of(
                              commitResponse,
                              commitResponse, // duplicate commit response
                              new Event(
                                      config.controlGroupId(),
                                      new TransactionDataComplete(
                                              currentCommitId,
                                              ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)),
                                              transactionsProcessed)));
                    });

    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.APPEND, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());
    Map<String, String> summary = snapshot.summary();
    Assertions.assertEquals(99L, Long.valueOf(summary.get(TX_ID_VALID_THROUGH_PROP)));
    Assertions.assertEquals(100L, Long.valueOf(summary.get(MAX_TX_ID_PROP)));
  }

  @Test
  public void testShouldDeduplicateDeleteFilesBeforeAppending() {
    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    DeleteFile deleteFile = EventTestUtil.createDeleteFile();

    // CHANGED: Use the new transaction object
    TableReference tableRef = TableReference.of("catalog", TableIdentifier.of("db", "tbl"));

    UUID commitId =
            coordinatorTest(
                    currentCommitId -> {
                      Event duplicateCommitResponse =
                              new Event(
                                      config.controlGroupId(),
                                      new DataWritten(
                                              StructType.of(),
                                              currentCommitId,
                                              tableRef,
                                              ImmutableList.of(),
                                              ImmutableList.of(deleteFile, deleteFile))); // duplicate delete files

                      return ImmutableList.of(
                              duplicateCommitResponse,
                              duplicateCommitResponse, // duplicate commit response
                              new Event(
                                      config.controlGroupId(),
                                      new TransactionDataComplete(
                                              currentCommitId,
                                              ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)),
                                              ImmutableList.of(new TableTopicPartitionTransaction("topic", 1, "catalog", TABLE_IDENTIFIER, 100L)))));
                    });

    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.DELETE, snapshot.operation());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());
  }

  private void validateAddedFiles(
          Snapshot snapshot, Set<String> expectedDataFilePaths, PartitionSpec expectedSpec) {
    final List<DataFile> addedDataFiles = ImmutableList.copyOf(snapshot.addedDataFiles(table.io()));
    final List<DeleteFile> addedDeleteFiles =
            ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io()));

    Assertions.assertEquals(
            expectedDataFilePaths,
            addedDataFiles.stream().map(ContentFile::path).collect(Collectors.toSet()));

    Assertions.assertEquals(
            ImmutableSet.of(expectedSpec.specId()),
            Stream.concat(addedDataFiles.stream(), addedDeleteFiles.stream())
                    .map(ContentFile::specId)
                    .collect(Collectors.toSet()));
  }

  @Test
  public void testTxIdValidThroughInSnapshotSummary() {
    Assertions.assertEquals(0, ImmutableList.copyOf(table.snapshots().iterator()).size());

    // CHANGED: This test now uses the richer object directly
    TableReference tableRef = TableReference.of("catalog", TABLE_IDENTIFIER);
    Map<TopicPartition, Long> txIdPerPartition = ImmutableMap.of(
            new TopicPartition("topic", 1), 100L,
            new TopicPartition("topic", 2), 102L);

    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    UUID commitId =
            coordinatorTxIdValidThroughTest(ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), ts, txIdPerPartition);
    table.refresh();

    assertThat(producer.history()).hasSize(3);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
            .isEqualTo(ImmutableMap.of(CTL_TOPIC_PARTITION, new OffsetAndMetadata(3L)));
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.APPEND, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    Map<String, String> summary = snapshot.summary();
    Assertions.assertEquals(commitId.toString(), summary.get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertEquals("{\"0\":3}", summary.get(OFFSETS_SNAPSHOT_PROP));
    Assertions.assertEquals(
            Long.toString(ts.toInstant().toEpochMilli()), summary.get(VTTS_SNAPSHOT_PROP));
    Assertions.assertEquals(99L, Long.valueOf(summary.get(TX_ID_VALID_THROUGH_PROP)));
    Assertions.assertEquals(102L, Long.valueOf(summary.get(MAX_TX_ID_PROP)));
  }

  @Test
  public void testCommitMultiPartitionSpecAppendDataFiles() {
    final PartitionSpec spec1 = table.spec();
    assert spec1.isUnpartitioned();

    // evolve spec to partition by date
    final PartitionSpec partitionByDate = PartitionSpec.builderFor(SCHEMA).identity("date").build();
    table.updateSpec().addField(partitionByDate.fields().get(0).name()).commit();
    final PartitionSpec spec2 = table.spec();
    assert spec2.isPartitioned();

    // pretend we have two workers each handling 1 topic partition
    final List<MemberDescription> members = Lists.newArrayList();
    for (int i : ImmutableList.of(0, 1)) {
      members.add(
              new MemberDescription(
                      "memberId" + i,
                      "clientId" + i,
                      "host" + i,
                      new MemberAssignment(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, i)))));
    }

    final Coordinator coordinator = new Coordinator(catalog, config, members, clientFactory);
    initConsumer();

    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    coordinator.process();

    final byte[] bytes = producer.history().get(0).value();
    final Event commitRequest = AvroUtil.decode(bytes);
    assert commitRequest.type().equals(PayloadType.START_COMMIT);
    final UUID commitId = ((StartCommit) commitRequest.payload()).commitId();

    Map<Integer, PartitionSpec> workerIdToSpecMap =
            ImmutableMap.of(
                    1, spec1,
                    2, spec2
            );

    // CHANGED: Use the new transaction object
    TableReference tableRef = TableReference.of("catalog", TABLE_IDENTIFIER);
    int currentControlTopicOffset = 1;
    for (Map.Entry<Integer, PartitionSpec> entry : workerIdToSpecMap.entrySet()) {
      Integer workerId = entry.getKey();
      PartitionSpec spec = entry.getValue();

      final DataFile dataFile =
              DataFiles.builder(spec)
                      .withPath(String.format("file%d.parquet", workerId))
                      .withFileSizeInBytes(100)
                      .withRecordCount(5)
                      .build();

      consumer.addRecord(
              new ConsumerRecord<>(
                      CTL_TOPIC_NAME,
                      0,
                      currentControlTopicOffset,
                      "key",
                      AvroUtil.encode(
                              new Event(
                                      config.controlGroupId(),
                                      new DataWritten(
                                              spec.partitionType(),
                                              commitId,
                                              tableRef,
                                              ImmutableList.of(dataFile),
                                              ImmutableList.of())))));
      currentControlTopicOffset += 1;

      consumer.addRecord(
              new ConsumerRecord<>(
                      CTL_TOPIC_NAME,
                      0,
                      currentControlTopicOffset,
                      "key",
                      AvroUtil.encode(
                              new Event(
                                      config.controlGroupId(),
                                      new TransactionDataComplete(
                                              commitId,
                                              ImmutableList.of(
                                                      new TopicPartitionOffset(
                                                              SRC_TOPIC_NAME,
                                                              0,
                                                              100L,
                                                              OffsetDateTime.ofInstant(
                                                                      Instant.ofEpochMilli(100L), ZoneOffset.UTC))),
                                              ImmutableList.of(
                                                      new TableTopicPartitionTransaction(SRC_TOPIC_NAME, 0, "catalog", TABLE_IDENTIFIER, 100L),
                                                      new TableTopicPartitionTransaction(SRC_TOPIC_NAME, 1, "catalog", TABLE_IDENTIFIER, 110L),
                                                      new TableTopicPartitionTransaction(SRC_TOPIC_NAME, 2, "catalog", TABLE_IDENTIFIER, 102L)))))));
      currentControlTopicOffset += 1;
    }

    coordinator.process();

    table.refresh();
    final List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(2, snapshots.size());

    final Snapshot firstSnapshot = snapshots.get(0);
    final Snapshot secondSnapshot = snapshots.get(1);

    validateAddedFiles(firstSnapshot, ImmutableSet.of("file1.parquet"), spec1);
    validateAddedFiles(secondSnapshot, ImmutableSet.of("file2.parquet"), spec2);

    Assertions.assertEquals(commitId.toString(), firstSnapshot.summary().get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertNull(firstSnapshot.summary().getOrDefault(OFFSETS_SNAPSHOT_PROP, null));
    Assertions.assertNull(firstSnapshot.summary().getOrDefault(VTTS_SNAPSHOT_PROP, null));

    Assertions.assertEquals(commitId.toString(), secondSnapshot.summary().get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertEquals("{\"0\":5}", secondSnapshot.summary().get(OFFSETS_SNAPSHOT_PROP));
    Assertions.assertEquals("100", secondSnapshot.summary().get(VTTS_SNAPSHOT_PROP));
    Assertions.assertEquals(99L, Long.valueOf(secondSnapshot.summary().get(TX_ID_VALID_THROUGH_PROP)));
    Assertions.assertEquals(110L, Long.valueOf(secondSnapshot.summary().get(MAX_TX_ID_PROP)));
  }

  private void assertCommitTable(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitTable = AvroUtil.decode(bytes);
    assertThat(commitTable.type()).isEqualTo(PayloadType.COMMIT_TO_TABLE);
    CommitToTable commitTablePayload = (CommitToTable) commitTable.payload();
    assertThat(commitTablePayload.commitId()).isEqualTo(commitId);
    assertThat(commitTablePayload.tableReference().identifier().toString())
            .isEqualTo(TABLE_IDENTIFIER.toString());
    assertThat(commitTablePayload.validThroughTs()).isEqualTo(ts);
  }

  private void assertCommitComplete(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitComplete = AvroUtil.decode(bytes);
    assertThat(commitComplete.type()).isEqualTo(PayloadType.COMMIT_COMPLETE);
    CommitComplete commitCompletePayload = (CommitComplete) commitComplete.payload();
    assertThat(commitCompletePayload.commitId()).isEqualTo(commitId);
    assertThat(commitCompletePayload.validThroughTs()).isEqualTo(ts);
  }

  // CHANGED: Helper method signature and implementation
  private UUID coordinatorTest(
          List<DataFile> dataFiles, List<DeleteFile> deleteFiles, OffsetDateTime ts, List<TableTopicPartitionTransaction> transactionsProcessed) {
    return coordinatorTest(
            currentCommitId -> {
              Event commitResponse =
                      new Event(
                              config.controlGroupId(),
                              new DataWritten(
                                      StructType.of(),
                                      currentCommitId,
                                      TableReference.of("catalog", TableIdentifier.of("db", "tbl")),
                                      dataFiles,
                                      deleteFiles));

              Event commitReady =
                      new Event(
                              config.controlGroupId(),
                              new TransactionDataComplete(
                                      currentCommitId,
                                      ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)),
                                      transactionsProcessed));

              return ImmutableList.of(commitResponse, commitReady);
            });
  }

  // CHANGED: Helper method implementation
  private UUID coordinatorTxIdValidThroughTest(
          List<DataFile> dataFiles, List<DeleteFile> deleteFiles, OffsetDateTime ts, Map<TopicPartition, Long> txIdPerPartition) {
    return coordinatorTest(
            currentCommitId -> {
              Event commitResponse =
                      new Event(
                              config.controlGroupId(),
                              new DataWritten(
                                      StructType.of(),
                                      currentCommitId,
                                      TableReference.of("catalog", TableIdentifier.of("db", "tbl")),
                                      dataFiles,
                                      deleteFiles));

              TableReference tableRef = TableReference.of("catalog", TableIdentifier.of("db", "tbl"));
              List<TableTopicPartitionTransaction> tableTxIds = txIdPerPartition.entrySet().stream()
                      .map(entry -> new TableTopicPartitionTransaction(entry.getKey().topic(), entry.getKey().partition(), "catalog", TABLE_IDENTIFIER, entry.getValue()))
                      .collect(Collectors.toList());

              Event commitReady =
                      new Event(
                              config.controlGroupId(),
                              new TransactionDataComplete(
                                      currentCommitId,
                                      ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)),
                                      tableTxIds));

              return ImmutableList.of(commitResponse, commitReady);
            });
  }

  private UUID coordinatorTest(Function<UUID, List<Event>> eventsFn) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    Coordinator coordinator = new Coordinator(catalog, config, ImmutableList.of(), clientFactory);

    initConsumer();
    coordinator.process();

    assertThat(producer.transactionCommitted()).isTrue();
    assertThat(producer.history()).hasSize(1);

    byte[] bytes = producer.history().get(0).value();
    Event commitRequest = AvroUtil.decode(bytes);
    assertThat(commitRequest.type()).isEqualTo(PayloadType.START_COMMIT);

    UUID commitId = ((StartCommit) commitRequest.payload()).commitId();

    int currentOffset = 1;
    for (Event event : eventsFn.apply(commitId)) {
      bytes = AvroUtil.encode(event);
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, currentOffset, "key", bytes));
      currentOffset += 1;
    }

    when(config.commitIntervalMs()).thenReturn(0);
    coordinator.process();

    return commitId;
  }
}
