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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.WriterResult;
import io.tabular.iceberg.connect.events.TableTopicPartitionTransaction;
import io.tabular.iceberg.connect.events.TransactionDataComplete;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference; // ADDED
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class CommitterImplTest {

  private static final String CATALOG_NAME = "iceberg";
  private static final String SOURCE_TOPIC = "source-topic-name";
  private static final TopicPartition SOURCE_TP0 = new TopicPartition(SOURCE_TOPIC, 0);
  private static final TopicPartition SOURCE_TP1 = new TopicPartition(SOURCE_TOPIC, 1);
  private static final Set<TopicPartition> ASSIGNED_SOURCE_TOPIC_PARTITIONS =
          ImmutableSet.of(SOURCE_TP0);
  private static final String CONNECTOR_NAME = "connector-name";
  private static final String TABLE_1_NAME = "db.tbl1";
  private static final TableIdentifier TABLE_1_IDENTIFIER = TableIdentifier.parse(TABLE_1_NAME);
  private static final TableReference TABLE_1_REFERENCE = TableReference.of(CATALOG_NAME, TABLE_1_IDENTIFIER); // ADDED
  private static final String CONTROL_TOPIC = "control-topic-name";
  private static final TopicPartition CONTROL_TOPIC_PARTITION =
          new TopicPartition(CONTROL_TOPIC, 0);
  private KafkaClientFactory kafkaClientFactory;
  private UUID producerId;
  private MockProducer<String, byte[]> producer;
  private MockConsumer<String, byte[]> consumer;
  private Admin admin;

  @BeforeEach
  public void before() {
    admin = mock(Admin.class);
    producerId = UUID.randomUUID();
    producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
    producer.initTransactions();
    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    kafkaClientFactory = mock(KafkaClientFactory.class);
    when(kafkaClientFactory.createConsumer(any())).thenReturn(consumer);
    when(kafkaClientFactory.createProducer(any())).thenReturn(Pair.of(producerId, producer));
    when(kafkaClientFactory.createAdmin()).thenReturn(admin);
  }

  @AfterEach
  public void after() {
    producer.close();
    consumer.close();
    admin.close();
  }

  private void initConsumer() {
    consumer.rebalance(ImmutableList.of(CONTROL_TOPIC_PARTITION));
    consumer.updateBeginningOffsets(ImmutableMap.of(CONTROL_TOPIC_PARTITION, 0L));
  }

  private static IcebergSinkConfig makeConfig(int taskId) {
    return new IcebergSinkConfig(
            ImmutableMap.of(
                    "name",
                    CONNECTOR_NAME,
                    "iceberg.catalog.catalog-impl",
                    "org.apache.iceberg.inmemory.InMemoryCatalog",
                    "iceberg.tables",
                    TABLE_1_NAME,
                    "iceberg.control.topic",
                    CONTROL_TOPIC,
                    IcebergSinkConfig.INTERNAL_TRANSACTIONAL_SUFFIX_PROP,
                    "-txn-" + UUID.randomUUID() + "-" + taskId));
  }

  private static final IcebergSinkConfig CONFIG = makeConfig(1);

  private SinkTaskContext mockContext() {
    SinkTaskContext mockContext = mock(SinkTaskContext.class);
    when(mockContext.assignment()).thenReturn(ASSIGNED_SOURCE_TOPIC_PARTITIONS);
    return mockContext;
  }

  private static DynConstructors.Ctor<CoordinatorKey> ctorCoordinatorKey() {
    return DynConstructors.builder(CoordinatorKey.class)
            .hiddenImpl(
                    "org.apache.kafka.clients.admin.internals.CoordinatorKey",
                    FindCoordinatorRequest.CoordinatorType.class,
                    String.class)
            .build();
  }

  private static DynConstructors.Ctor<ListConsumerGroupOffsetsResult>
  ctorListConsumerGroupOffsetsResult() {
    return DynConstructors.builder(ListConsumerGroupOffsetsResult.class)
            .hiddenImpl("org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult", Map.class)
            .build();
  }

  private final CoordinatorKey coordinatorKey =
          ctorCoordinatorKey()
                  .newInstance(FindCoordinatorRequest.CoordinatorType.GROUP, "fakeCoordinatorKey");

  @SuppressWarnings("deprecation")
  private static ListConsumerGroupOffsetsOptions listOffsetResultMatcher() {
    return argThat(x -> x.topicPartitions() == null && x.requireStable());
  }

  private ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult(
          Map<TopicPartition, Long> consumerOffsets) {
    return ctorListConsumerGroupOffsetsResult()
            .newInstance(
                    ImmutableMap.of(
                            coordinatorKey,
                            KafkaFuture.completedFuture(
                                    consumerOffsets.entrySet().stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue()))))));
  }

  private void whenAdminListConsumerGroupOffsetsThenReturn(
          Map<String, Map<TopicPartition, Long>> consumersOffsets) {
    consumersOffsets.forEach(
            (consumerGroup, consumerOffsets) -> {
              when(admin.listConsumerGroupOffsets(eq(consumerGroup), listOffsetResultMatcher()))
                      .thenReturn(listConsumerGroupOffsetsResult(consumerOffsets));
            });
  }

  private static class NoOpCoordinatorThreadFactory implements CoordinatorThreadFactory {
    @Override
    public Optional<CoordinatorThread> create(SinkTaskContext context, IcebergSinkConfig config) {
      CoordinatorThread mockThread = mock(CoordinatorThread.class);
      Mockito.doNothing().when(mockThread).start();
      Mockito.doNothing().when(mockThread).terminate();
      return Optional.of(mockThread);
    }
  }

  private static class TerminatedCoordinatorThreadFactory implements CoordinatorThreadFactory {
    @Override
    public Optional<CoordinatorThread> create(SinkTaskContext context, IcebergSinkConfig config) {
      CoordinatorThread mockThread = mock(CoordinatorThread.class);
      Mockito.doNothing().when(mockThread).start();
      Mockito.doNothing().when(mockThread).terminate();
      Mockito.doReturn(true).when(mockThread).isTerminated();
      return Optional.of(mockThread);
    }
  }

  private static <F> String toPath(ContentFile<F> contentFile) {
    return contentFile.path().toString();
  }

  private static <F extends ContentFile<F>> void assertSameContentFiles(
          List<F> actual, List<F> expected) {
    assertThat(actual.stream().map(CommitterImplTest::toPath).collect(Collectors.toList()))
            .containsExactlyElementsOf(
                    expected.stream().map(CommitterImplTest::toPath).collect(Collectors.toList()));
  }

  private void assertDataWritten(
          ProducerRecord<String, byte[]> producerRecord,
          UUID expectedCommitId,
          TableIdentifier expectedTableIdentifier,
          List<DataFile> expectedDataFiles,
          List<DeleteFile> expectedDeleteFiles) {
    assertThat(producerRecord.key()).isEqualTo(producerId.toString());
    Event event = AvroUtil.decode(producerRecord.value());
    assertThat(event.type()).isEqualTo(PayloadType.DATA_WRITTEN);
    assertThat(event.payload()).isInstanceOf(DataWritten.class);
    DataWritten payload = (DataWritten) event.payload();
    assertThat(payload.commitId()).isEqualTo(expectedCommitId);
    assertThat(payload.tableReference().identifier()).isEqualTo(expectedTableIdentifier);
    assertThat(payload.tableReference().catalog()).isEqualTo(CATALOG_NAME);
    assertSameContentFiles(payload.dataFiles(), expectedDataFiles);
    assertSameContentFiles(payload.deleteFiles(), expectedDeleteFiles);
  }

  // CHANGED: Helper method updated to assert on the new event structure
  private void assertDataComplete(
          ProducerRecord<String, byte[]> producerRecord,
          UUID expectedCommitId,
          Map<TopicPartition, Pair<Long, OffsetDateTime>> expectedAssignments,
          List<TableTopicPartitionTransaction> expectedTxIds) {
    assertThat(producerRecord.key()).isEqualTo(producerId.toString());

    Event event = AvroUtil.decode(producerRecord.value());
    assertThat(event.type()).isEqualTo(PayloadType.DATA_COMPLETE);
    assertThat(event.payload()).isInstanceOf(TransactionDataComplete.class);
    TransactionDataComplete commitReadyPayload = (TransactionDataComplete) event.payload();
    assertThat(commitReadyPayload.commitId()).isEqualTo(expectedCommitId);
    assertThat(
            commitReadyPayload.assignments().stream()
                    .map(
                            x ->
                                    Pair.of(
                                            new TopicPartition(x.topic(), x.partition()),
                                            Pair.of(x.offset(), x.timestamp())))
                    .collect(Collectors.toList()))
            .containsExactlyInAnyOrderElementsOf(
                    expectedAssignments.entrySet().stream()
                            .map(e -> Pair.of(e.getKey(), e.getValue()))
                            .collect(Collectors.toList()));

    assertThat(commitReadyPayload.tableTxIds()).hasSize(expectedTxIds.size());
    for (int i = 0; i < expectedTxIds.size(); i++) {
      TableTopicPartitionTransaction actual = commitReadyPayload.tableTxIds().get(i);
      TableTopicPartitionTransaction expected = expectedTxIds.get(i);
      assertThat(actual.topic()).isEqualTo(expected.topic());
      assertThat(actual.partition()).isEqualTo(expected.partition());
      assertThat(actual.tableIdentifier()).isEqualTo(expected.tableIdentifier());
      assertThat(actual.txId()).isEqualTo(expected.txId());
    }
  }

  private OffsetDateTime offsetDateTime(Long ms) {
    return ms == null ? null : OffsetDateTime.ofInstant(Instant.ofEpochMilli(ms), ZoneOffset.UTC);
  }

  @Test
  public void testShouldRewindOffsetsToStableControlGroupConsumerOffsetsForAssignedPartitionsOnConstruction() throws IOException {
    SinkTaskContext mockContext = mockContext();
    ArgumentCaptor<Map<TopicPartition, Long>> offsetArgumentCaptor =
            ArgumentCaptor.forClass(Map.class);
    IcebergSinkConfig config = makeConfig(1);
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();
    whenAdminListConsumerGroupOffsetsThenReturn(
            ImmutableMap.of(
                    config.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L),
                    config.connectGroupId(), ImmutableMap.of(SOURCE_TP0, 90L, SOURCE_TP1, 80L)));

    try (CommitterImpl ignored =
                 new CommitterImpl(mockContext, config, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      verify(mockContext).offset(offsetArgumentCaptor.capture());
      assertThat(offsetArgumentCaptor.getAllValues())
              .isEqualTo(ImmutableList.of(ImmutableMap.of(SOURCE_TP0, 110L)));
    }
  }

  @Test
  public void testCommitShouldThrowExceptionIfCoordinatorIsTerminated() throws IOException {
    SinkTaskContext mockContext = mockContext();
    IcebergSinkConfig config = makeConfig(0);
    whenAdminListConsumerGroupOffsetsThenReturn(
            ImmutableMap.of(
                    config.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));
    TerminatedCoordinatorThreadFactory coordinatorThreadFactory =
            new TerminatedCoordinatorThreadFactory();
    CommittableSupplier committableSupplier = () -> { throw new NotImplementedException("Should not be called"); };

    try (CommitterImpl committerImpl =
                 new CommitterImpl(mockContext, config, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;
      assertThatThrownBy(() -> committer.commit(committableSupplier))
              .isInstanceOf(RuntimeException.class)
              .hasMessage("Coordinator unexpectedly terminated");
      assertThat(producer.history()).isEmpty();
      assertThat(producer.consumerGroupOffsetsHistory()).isEmpty();
    }
  }

  // Other tests (like testCommitShouldDoNothing...) remain the same as they don't involve Committables

  @Test
  public void testCommitShouldRespondToCommitRequest() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();
    UUID commitId = UUID.randomUUID();
    whenAdminListConsumerGroupOffsetsThenReturn(
            ImmutableMap.of(
                    CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    List<DataFile> dataFiles = ImmutableList.of(mock(DataFile.class));
    List<DeleteFile> deleteFiles = ImmutableList.of();
    Types.StructType partitionStruct = Types.StructType.of();
    Map<TopicPartition, Offset> sourceOffsets = ImmutableMap.of(SOURCE_TP0, new Offset(100L, 200L));

    // CHANGED: Create the new transaction object list
    List<TableTopicPartitionTransaction> sourceTxIds = ImmutableList.of(
            new TableTopicPartitionTransaction(SOURCE_TP0.topic(), SOURCE_TP0.partition(), CATALOG_NAME, TABLE_1_IDENTIFIER, 100L)
    );

    // CHANGED: Use the new list to construct the Committable
    CommittableSupplier committableSupplier =
            () ->
                    new Committable(
                            sourceOffsets,
                            sourceTxIds,
                            ImmutableList.of(
                                    new WriterResult(TABLE_1_IDENTIFIER, dataFiles, deleteFiles, partitionStruct)));

    try (CommitterImpl committerImpl =
                 new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      consumer.addRecord(
              new ConsumerRecord<>(
                      CONTROL_TOPIC_PARTITION.topic(),
                      CONTROL_TOPIC_PARTITION.partition(),
                      0,
                      UUID.randomUUID().toString(),
                      AvroUtil.encode(
                              new Event(
                                      CONFIG.controlGroupId(),
                                      new StartCommit(commitId)))));

      committer.commit(committableSupplier);

      assertThat(producer.transactionCommitted()).isTrue();
      assertThat(producer.history()).hasSize(2);
      assertDataWritten(
              producer.history().get(0),
              commitId,
              TABLE_1_IDENTIFIER,
              dataFiles,
              deleteFiles);
      // CHANGED: Assert using the new list
      assertDataComplete(
              producer.history().get(1),
              commitId,
              ImmutableMap.of(SOURCE_TP0, Pair.of(100L, offsetDateTime(200L))),
              sourceTxIds);

      assertThat(producer.consumerGroupOffsetsHistory()).hasSize(2);
      Map<TopicPartition, OffsetAndMetadata> expectedConsumerOffset =
              ImmutableMap.of(SOURCE_TP0, new OffsetAndMetadata(100L));
      assertThat(producer.consumerGroupOffsetsHistory().get(0))
              .isEqualTo(ImmutableMap.of(CONFIG.controlGroupId(), expectedConsumerOffset));
      assertThat(producer.consumerGroupOffsetsHistory().get(1))
              .isEqualTo(ImmutableMap.of(CONFIG.connectGroupId(), expectedConsumerOffset));
    }
  }

  @Test
  public void testCommitWhenCommittableIsEmpty() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();
    UUID commitId = UUID.randomUUID();
    whenAdminListConsumerGroupOffsetsThenReturn(
            ImmutableMap.of(
                    CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    // CHANGED: Use an empty list for the new Committable constructor
    CommittableSupplier committableSupplier =
            () -> new Committable(ImmutableMap.of(), ImmutableList.of(), ImmutableList.of());

    try (CommitterImpl committerImpl =
                 new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      consumer.addRecord(
              new ConsumerRecord<>(
                      CONTROL_TOPIC_PARTITION.topic(),
                      CONTROL_TOPIC_PARTITION.partition(),
                      0,
                      UUID.randomUUID().toString(),
                      AvroUtil.encode(
                              new Event(
                                      CONFIG.controlGroupId(),
                                      new StartCommit(commitId)))));


      committer.commit(committableSupplier);

      assertThat(producer.transactionCommitted()).isTrue();
      assertThat(producer.history()).hasSize(1);
      assertDataComplete(
              producer.history().get(0),
              commitId,
              ImmutableMap.of(SOURCE_TP0, Pair.of(null, null)),
              ImmutableList.of()); // CHANGED: Assert with empty list

      assertThat(producer.consumerGroupOffsetsHistory()).hasSize(0);
    }
  }

  @Test
  public void testCommitShouldCommitOffsetsOnlyForPartitionsWeMadeProgressOn() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();
    TopicPartition sourceTp0 = new TopicPartition(SOURCE_TOPIC, 0);
    TopicPartition sourceTp1 = new TopicPartition(SOURCE_TOPIC, 1);
    Set<TopicPartition> sourceTopicPartitions = ImmutableSet.of(sourceTp0, sourceTp1);
    when(mockContext.assignment()).thenReturn(sourceTopicPartitions);
    UUID commitId = UUID.randomUUID();
    whenAdminListConsumerGroupOffsetsThenReturn(
            ImmutableMap.of(
                    CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    List<DataFile> dataFiles = ImmutableList.of(mock(DataFile.class));
    List<DeleteFile> deleteFiles = ImmutableList.of();
    Types.StructType partitionStruct = Types.StructType.of();

    // CHANGED: Create the new transaction object list
    List<TableTopicPartitionTransaction> sourceTxIds = ImmutableList.of(
            new TableTopicPartitionTransaction(SOURCE_TP0.topic(), SOURCE_TP0.partition(), CATALOG_NAME, TABLE_1_IDENTIFIER, 100L)
    );

    // CHANGED: Use the new list to construct the Committable
    CommittableSupplier committableSupplier =
            () ->
                    new Committable(
                            ImmutableMap.of(sourceTp1, new Offset(100L, 200L)),
                            sourceTxIds,
                            ImmutableList.of(
                                    new WriterResult(TABLE_1_IDENTIFIER, dataFiles, deleteFiles, partitionStruct)));

    try (CommitterImpl committerImpl =
                 new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      consumer.addRecord(
              new ConsumerRecord<>(
                      CONTROL_TOPIC_PARTITION.topic(),
                      CONTROL_TOPIC_PARTITION.partition(),
                      0,
                      UUID.randomUUID().toString(),
                      AvroUtil.encode(
                              new Event(
                                      CONFIG.controlGroupId(),
                                      new StartCommit(commitId)))));

      committer.commit(committableSupplier);

      assertThat(producer.transactionCommitted()).isTrue();
      assertThat(producer.history()).hasSize(2);
      assertDataWritten(
              producer.history().get(0),
              commitId,
              TABLE_1_IDENTIFIER,
              dataFiles,
              deleteFiles);
      // CHANGED: Assert using the new list
      assertDataComplete(
              producer.history().get(1),
              commitId,
              ImmutableMap.of(
                      sourceTp0, Pair.of(null, null),
                      sourceTp1, Pair.of(100L, offsetDateTime(200L))),
              sourceTxIds);

      assertThat(producer.consumerGroupOffsetsHistory()).hasSize(2);
      Map<TopicPartition, OffsetAndMetadata> expectedConsumerOffset =
              ImmutableMap.of(sourceTp1, new OffsetAndMetadata(100L));
      assertThat(producer.consumerGroupOffsetsHistory().get(0))
              .isEqualTo(ImmutableMap.of(CONFIG.controlGroupId(), expectedConsumerOffset));
      assertThat(producer.consumerGroupOffsetsHistory().get(1))
              .isEqualTo(ImmutableMap.of(CONFIG.connectGroupId(), expectedConsumerOffset));
    }
  }
}
