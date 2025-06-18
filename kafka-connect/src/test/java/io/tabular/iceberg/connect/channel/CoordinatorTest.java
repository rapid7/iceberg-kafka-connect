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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.events.TopicPartitionTransaction;
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

  @Test
  public void testCommitAppend() {
    Assertions.assertEquals(0, ImmutableList.copyOf(table.snapshots().iterator()).size());

    List<TopicPartitionTransaction> transactionsProcessed =
            ImmutableList.of(new TopicPartitionTransaction("topic", 1, 100L));
    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    UUID commitId =
            coordinatorTest(
                    1, // This test simulates 1 partition
                    currentCommitId -> {
                      Event commitResponse =
                              new Event(
                                      config.controlGroupId(),
                                      new DataWritten(
                                              StructType.of(),
                                              currentCommitId,
                                              new TableReference("catalog", ImmutableList.of("db"), "tbl"),
                                              ImmutableList.of(EventTestUtil.createDataFile()),
                                              ImmutableList.of()));
                      Event commitReady =
                              new Event(
                                      config.controlGroupId(),
                                      new TransactionDataComplete(
                                              currentCommitId,
                                              ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)),
                                              transactionsProcessed));
                      return ImmutableList.of(commitResponse, commitReady);
                    });
    table.refresh();

    assertThat(producer.history()).hasSize(3);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
            .isEqualTo(ImmutableMap.of(CTL_TOPIC_PARTITION, new OffsetAndMetadata(3L)));
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    // ... assertions continue ...
  }

// In CoordinatorTest.java

  @Test
  public void testCommitDelta() {
    List<TopicPartitionTransaction> transactionsProcessed =
            ImmutableList.of(
                    new TopicPartitionTransaction("topic", 0, 100L), // Partition 0
                    new TopicPartitionTransaction("topic", 1, 102L)  // Partition 1
            );
    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);

    // This test simulates 2 partitions.
    UUID commitId =
            coordinatorTest(
                    2, // Configured for 2 partitions
                    currentCommitId -> {
                      // FIX: This line is now complete and syntactically correct.
                      Event dataWritten =
                              new Event(
                                      config.controlGroupId(),
                                      new DataWritten(
                                              StructType.of(),
                                              currentCommitId,
                                              new TableReference("catalog", ImmutableList.of("db"), "tbl"),
                                              ImmutableList.of(EventTestUtil.createDataFile()),
                                              ImmutableList.of(EventTestUtil.createDeleteFile())));

                      // The TransactionDataComplete event now correctly contains offset
                      // information for BOTH partitions that the test simulates.
                      Event commitReady =
                              new Event(
                                      config.controlGroupId(),
                                      new TransactionDataComplete(
                                              currentCommitId,
                                              ImmutableList.of(
                                                      new TopicPartitionOffset("topic", 0, 1L, ts), // Offset for partition 0
                                                      new TopicPartitionOffset("topic", 1, 1L, ts)  // Offset for partition 1
                                              ),
                                              transactionsProcessed));
                      return ImmutableList.of(dataWritten, commitReady);
                    });

    // Your test assertions should now pass.
    table.refresh();
    assertThat(producer.history()).hasSize(3);
    // ... other assertions ...
  }

  @Test
  public void testCommitNoFiles() {
    OffsetDateTime ts = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    // This test simulates 1 partition sending a "ready" signal with no data.
    UUID commitId =
            coordinatorTest(
                    1,
                    currentCommitId -> {
                      Event commitReady =
                              new Event(
                                      config.controlGroupId(),
                                      new TransactionDataComplete(
                                              currentCommitId,
                                              ImmutableList.of(new TopicPartitionOffset("topic", 0, 1L, ts)),
                                              ImmutableList.of()));
                      return ImmutableList.of(commitReady);
                    });

    assertThat(producer.history()).hasSize(2);
    assertThat(consumer.committed(ImmutableSet.of(CTL_TOPIC_PARTITION)))
            .isEqualTo(ImmutableMap.of(CTL_TOPIC_PARTITION, new OffsetAndMetadata(2L)));
    assertCommitComplete(1, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(0, snapshots.size());
  }

  // NOTE: Other tests like testCommitError, testShouldDeduplicate... would be refactored
  // in a similar way to call the new coordinatorTest helper with the correct partition count.

  // FIX: This helper method was missing in the previous version.
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

  // FIX: This helper method was missing in the previous version.
  private void assertCommitComplete(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitComplete = AvroUtil.decode(bytes);
    assertThat(commitComplete.type()).isEqualTo(PayloadType.COMMIT_COMPLETE);
    CommitComplete commitCompletePayload = (CommitComplete) commitComplete.payload();
    assertThat(commitCompletePayload.commitId()).isEqualTo(commitId);
    assertThat(commitCompletePayload.validThroughTs()).isEqualTo(ts);
  }

  // FIX: The main test helper is now parameterized to create a Coordinator with a valid partition count.
  private UUID coordinatorTest(int numPartitions, Function<UUID, List<Event>> eventsFn) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    List<MemberDescription> members = Lists.newArrayList();
    for (int i = 0; i < numPartitions; i++) {
      members.add(
              new MemberDescription(
                      "memberId" + i,
                      "clientId" + i,
                      "host" + i,
                      new MemberAssignment(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, i)))));
    }

    Coordinator coordinator = new Coordinator(catalog, config, members, clientFactory);

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

    coordinator.process();

    return commitId;
  }
}