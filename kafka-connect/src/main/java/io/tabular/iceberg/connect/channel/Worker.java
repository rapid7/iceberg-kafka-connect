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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.RecordWriter;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.data.WriterResult;
import io.tabular.iceberg.connect.events.TableTopicPartitionTransaction;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Worker implements Writer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private static final String COL_TXID = "txid";
  private final IcebergSinkConfig config;
  private final IcebergWriterFactory writerFactory;

  // State is now stored in maps keyed by the commit ID to isolate data per-commit.
  private volatile UUID currentCommitId;
  private final Map<UUID, Map<String, RecordWriter>> writersByCommit = Maps.newConcurrentMap();
  private final Map<UUID, Map<TopicPartition, Offset>> sourceOffsetsByCommit = Maps.newConcurrentMap();
  private final Map<UUID, Map<TableIdentifier, Map<TopicPartition, Long>>> txIdsByCommit =
          Maps.newConcurrentMap();

  Worker(IcebergSinkConfig config, Catalog catalog) {
    this(config, new IcebergWriterFactory(catalog, config));
  }

  @VisibleForTesting
  Worker(IcebergSinkConfig config, IcebergWriterFactory writerFactory) {
    this.config = config;
    this.writerFactory = writerFactory;
  }

  /**
   * Sets the active commit ID for which records should be collected. This method MUST be called
   * when a `StartCommit` event is received, before any subsequent records are written.
   *
   * @param commitId the commit ID
   */
  public synchronized void setCurrentCommitId(UUID commitId) {
    LOG.info("Starting collection for new commit ID: {}", commitId);
    this.currentCommitId = commitId;
  }

  @Override
  public synchronized Committable committable(UUID commitId) {
    // Retrieve the state for the specified commit and remove it from the maps.
    // The committer thread now has exclusive ownership of this state.
    Map<String, RecordWriter> writersForCommit = writersByCommit.remove(commitId);
    Map<TopicPartition, Offset> offsetsForCommit = sourceOffsetsByCommit.remove(commitId);
    Map<TableIdentifier, Map<TopicPartition, Long>> txIdsForCommit = txIdsByCommit.remove(commitId);

    if (writersForCommit == null && offsetsForCommit == null && txIdsForCommit == null) {
      // No data was received for this commit.
      return new Committable(Maps.newHashMap(), Lists.newArrayList(), Lists.newArrayList());
    }
    // Complete the writers and gather the results
    List<WriterResult> writeResults =
            writersForCommit.values().stream()
                    .flatMap(writer -> writer.complete().stream())
                    .collect(toList());
    // Gather the transaction IDs
    List<TableTopicPartitionTransaction> tableTxIds = Lists.newArrayList();
    if (txIdsForCommit != null) {
      txIdsForCommit.forEach(
              (tableIdentifier, partitionTxIds) -> {
                String catalogName = config.catalogName();
                partitionTxIds.forEach(
                        (tp, txId) ->
                                tableTxIds.add(
                                        new TableTopicPartitionTransaction(
                                                tp.topic(), tp.partition(), catalogName, tableIdentifier, txId)));
              });
    }

    LOG.info("Created committable for commit ID {} with {} table txns", commitId, tableTxIds.size());
    return new Committable(offsetsForCommit, tableTxIds, writeResults);
  }

  @Override
  public synchronized void close() throws IOException {
    writersByCommit.values().forEach(map -> map.values().forEach(RecordWriter::close));
    writersByCommit.clear();
    sourceOffsetsByCommit.clear();
    txIdsByCommit.clear();
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      sinkRecords.forEach(this::save);
    }
  }

  private synchronized void save(SinkRecord record) {
    if (currentCommitId == null) {
      LOG.warn(
              "No active commit in progress, skipping record. Topic: {}, Partition: {}, Offset: {}",
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset());
      return;
    }

    Map<TopicPartition, Offset> currentOffsets =
            sourceOffsetsByCommit.computeIfAbsent(currentCommitId, k -> Maps.newConcurrentMap());
    currentOffsets.put(
            new TopicPartition(record.topic(), record.kafkaPartition()),
            new Offset(record.kafkaOffset() + 1, record.timestamp()));

    String routeValue;
    if (config.dynamicTablesEnabled()) {
      String routeField = config.tablesRouteField();
      Preconditions.checkNotNull(routeField, "Route field cannot be null with dynamic routing");
      routeValue = extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        String tableName = routeValue.toLowerCase();
        writerForTable(currentCommitId, tableName, record, true).write(record);
        updateTxId(currentCommitId, tableName, record);
      }
    } else {
      String routeField = config.tablesRouteField();
      if (routeField == null) {
        config
                .tables()
                .forEach(
                        tableName -> {
                          writerForTable(currentCommitId, tableName, record, false).write(record);
                          updateTxId(currentCommitId, tableName, record);
                        });
      } else {
        routeValue = extractRouteValue(record.value(), routeField);
        if (routeValue != null) {
          config
                  .tables()
                  .forEach(
                          tableName ->
                                  config
                                          .tableConfig(tableName)
                                          .routeRegex()
                                          .ifPresent(
                                                  regex -> {
                                                    if (regex.matcher(routeValue).matches()) {
                                                      writerForTable(currentCommitId, tableName, record, false)
                                                              .write(record);
                                                      updateTxId(currentCommitId, tableName, record);
                                                    }
                                                  }));
        }
      }
    }
  }

  private void updateTxId(UUID commitId, String tableName, SinkRecord record) {
    Long txId = Utilities.extractTxIdFromRecordValue(record.value(), COL_TXID);
    if (txId != null) {
      Map<TableIdentifier, Map<TopicPartition, Long>> currentCommitTxIds =
              txIdsByCommit.computeIfAbsent(commitId, k -> Maps.newConcurrentMap());

      TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
      Map<TopicPartition, Long> partitionTxIds =
              currentCommitTxIds.computeIfAbsent(tableIdentifier, k -> Maps.newHashMap());
      TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
      partitionTxIds.merge(tp, txId, Long::max);
    }
  }

  private String extractRouteValue(Object recordValue, String routeField) {
    if (recordValue == null) {
      return null;
    }
    Object routeValue = Utilities.extractFromRecordValue(recordValue, routeField);
    return routeValue == null ? null : routeValue.toString();
  }

  private RecordWriter writerForTable(
          UUID commitId, String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    Map<String, RecordWriter> currentWriters =
            writersByCommit.computeIfAbsent(commitId, k -> Maps.newConcurrentMap());
    return currentWriters.computeIfAbsent(
            tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
