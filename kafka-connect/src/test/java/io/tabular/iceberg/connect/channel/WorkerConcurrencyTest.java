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
//package io.tabular.iceberg.connect.channel;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.ArgumentMatchers.anyBoolean;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//import com.google.common.collect.ImmutableList;
//import io.tabular.iceberg.connect.IcebergSinkConfig;
//import io.tabular.iceberg.connect.data.IcebergWriterFactory;
//import io.tabular.iceberg.connect.data.RecordWriter;
//import io.tabular.iceberg.connect.data.WriterResult;
//import io.tabular.iceberg.connect.events.TableTopicPartitionTransaction;
//
//import java.util.Collections;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//import java.util.stream.Collectors;
//
//import org.apache.iceberg.DataFile;
//import org.apache.iceberg.catalog.TableIdentifier;
//import org.apache.iceberg.types.Types;
//import org.apache.kafka.connect.data.Schema;
//import org.apache.kafka.connect.data.SchemaBuilder;
//import org.apache.kafka.connect.data.Struct;
//import org.apache.kafka.connect.sink.SinkRecord;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//@ExtendWith(MockitoExtension.class)
//class WorkerConcurrencyTest {
//
//    @Mock private IcebergSinkConfig mockConfig;
//    @Mock private IcebergWriterFactory mockWriterFactory;
//    @Mock private RecordWriter mockRecordWriter;
//
//    private Worker worker;
//
//    @BeforeEach
//    void setup() {
//        when(mockWriterFactory.createWriter(any(), any(), anyBoolean())).thenReturn(mockRecordWriter);
//
//        when(mockConfig.dynamicTablesEnabled()).thenReturn(false);
//        when(mockConfig.tablesRouteField()).thenReturn(null);
//        when(mockConfig.tables()).thenReturn(Collections.singletonList("db.tbl"));
//
//        worker = new Worker(mockConfig, mockWriterFactory);
//    }
//
//    private SinkRecord createRecord(long txid, long offset) {
//        Schema valueSchema = SchemaBuilder.struct().field("txid", Schema.INT64_SCHEMA).build();
//        Struct value = new Struct(valueSchema).put("txid", txid);
//        when(mockConfig.catalogName()).thenReturn("test_catalog");
//        return new SinkRecord("src-topic", 0, null, null, valueSchema, value, offset);
//    }
//
//    @Test
//    void committableShouldBeThreadSafeAndIsolateState() throws Exception {
//        SinkRecord recordFromBatch1 = createRecord(100L, 1L);
//        SinkRecord recordFromBatch2 = createRecord(999L, 2L);
//
//        // Mock the writer to return a valid result when its complete() method is called.
//        DataFile mockDataFile = mock(DataFile.class);
//        WriterResult mockResult = new WriterResult(TableIdentifier.of("db", "tbl"), List.of(mockDataFile), ImmutableList.of(), Types.StructType.of());
//        // We configure the mock to return a result only ONCE.
//        when(mockRecordWriter.complete()).thenReturn(List.of(mockResult));
//
//        ExecutorService executor = Executors.newFixedThreadPool(2);
//        CountDownLatch latch = new CountDownLatch(1);
//
//        // 2. DEFINE THREAD A (simulating the Kafka Connect `put` thread)
//        // This thread writes the first batch, signals the committer, then
//        // immediately tries to write the second batch to create the race.
//        Future<?> putterThreadFuture = executor.submit(() -> {
//            try {
//                // Process the first batch of records.
//                worker.write(List.of(recordFromBatch1));
//                latch.countDown();
//                Thread.sleep(50);
//                worker.write(List.of(recordFromBatch2));
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//        });
//
//        // 3. DEFINE THREAD B (simulating the `Committer` thread)
//        Future<Committable> committerThreadFuture = executor.submit(() -> {
//            // Wait for the signal that the first batch has been written.
//            latch.await();
//            return worker.committable();
//        });
//
//        // 4. EXECUTE AND GET RESULTS
//        Committable firstCommit = committerThreadFuture.get();
//        putterThreadFuture.get(); // Ensure the putter thread also completes.
//
//        // 5. ASSERT THE RESULTS OF THE FIRST COMMIT
//        // The `synchronized` keyword should have protected the worker. Depending on thread
//        // scheduling, the first commit will contain EITHER batch 1 OR both batches.
//        // The key is that the worker's state remains consistent.
//
//        // After the first commit, the second batch should be the only thing left.
//        Committable secondCommit = worker.committable();
//
//        // We can now assert on the contents of the two commits combined.
//        // One of them must have batch 1, the other must have batch 2.
//        Committable commitWithBatch1 = firstCommit.getTableTxIds().isEmpty() ? secondCommit : firstCommit;
//        Committable commitWithBatch2 = firstCommit.getTableTxIds().isEmpty() ? firstCommit : secondCommit;
//
//        if (commitWithBatch1.getTableTxIds().size() == 2) {
//            // This means the putter thread was faster and both batches were in the first commit.
//            // The second commit should be empty.
//            assertThat(secondCommit.getTableTxIds()).isEmpty();
//            List<Long> txIds = commitWithBatch1.getTableTxIds().stream()
//                    .map(TableTopicPartitionTransaction::txId)
//                    .collect(Collectors.toList());
//            assertThat(txIds).containsExactlyInAnyOrder(100L, 999L);
//        } else {
//            // This means the committer was faster and correctly isolated the first batch.
//            // The second commit should contain the second batch.
//            assertThat(commitWithBatch1.getTableTxIds()).hasSize(1);
//            assertThat(commitWithBatch1.getTableTxIds().get(0).txId()).isEqualTo(100L);
//            assertThat(commitWithBatch2.getTableTxIds()).hasSize(1);
//            assertThat(commitWithBatch2.getTableTxIds().get(0).txId()).isEqualTo(999L);
//        }
//
//        executor.shutdown();
//    }
//}