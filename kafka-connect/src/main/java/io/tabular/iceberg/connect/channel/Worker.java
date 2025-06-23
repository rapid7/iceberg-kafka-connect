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
  private final Map<String, RecordWriter> writers;
  private final Map<TopicPartition, Offset> sourceOffsets;
  private final Map<TableIdentifier, Map<TopicPartition, Long>> txIdsByTable;

  Worker(IcebergSinkConfig config, Catalog catalog) {
    this(config, new IcebergWriterFactory(catalog, config));
  }

  @VisibleForTesting
  Worker(IcebergSinkConfig config, IcebergWriterFactory writerFactory) {
    this.config = config;
    this.writerFactory = writerFactory;
    this.writers = Maps.newHashMap();
    this.sourceOffsets = Maps.newHashMap();
    this.txIdsByTable = Maps.newHashMap();
  }

  @Override
  public Committable committable() {
    List<WriterResult> writeResults =
            writers.values().stream().flatMap(writer -> writer.complete().stream()).collect(toList());
    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    // --- THIS IS THE FIX ---
    // The logic to flatten the map into a list is updated to call the correct constructor.
    List<TableTopicPartitionTransaction> tableTxIds = Lists.newArrayList();
    txIdsByTable.forEach((tableIdentifier, partitionTxIds) -> {
      String catalogName = config.catalogName(); // Get the catalog name string
      partitionTxIds.forEach((tp, txId) ->
              // Call the constructor with (topic, partition, catalogName, tableIdentifier, txId)
              tableTxIds.add(new TableTopicPartitionTransaction(
                      tp.topic(), tp.partition(), catalogName, tableIdentifier, txId))
      );
    });
    // --- END FIX ---

    LOG.debug("TRACE: Committable committed table partition txIds {}", tableTxIds);
    LOG.debug("TRACE: Committable committable txIds {}", txIdsByTable);

    writers.clear();
    sourceOffsets.clear();
    txIdsByTable.clear();

    return new Committable(offsets, tableTxIds, writeResults);
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
    txIdsByTable.clear();
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      sinkRecords.forEach(this::save);
    }
  }

  private void save(SinkRecord record) {
    sourceOffsets.put(
            new TopicPartition(record.topic(), record.kafkaPartition()),
            new Offset(record.kafkaOffset() + 1, record.timestamp()));

    String routeValue;
    if (config.dynamicTablesEnabled()) {
      String routeField = config.tablesRouteField();
      Preconditions.checkNotNull(routeField, "Route field cannot be null with dynamic routing");
      routeValue = extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        String tableName = routeValue.toLowerCase();
        writerForTable(tableName, record, true).write(record);
        updateTxId(tableName, record);
      }
    } else {
      String routeField = config.tablesRouteField();
      if (routeField == null) {
        config.tables().forEach(tableName -> {
          writerForTable(tableName, record, false).write(record);
          updateTxId(tableName, record);
        });
      } else {
        routeValue = extractRouteValue(record.value(), routeField);
        if (routeValue != null) {
          config.tables().forEach(tableName ->
                  config.tableConfig(tableName).routeRegex().ifPresent(regex -> {
                    if (regex.matcher(routeValue).matches()) {
                      writerForTable(tableName, record, false).write(record);
                      updateTxId(tableName, record);
                    }
                  })
          );
        }
      }
    }
  }

  private void updateTxId(String tableName, SinkRecord record) {
    Long txId = Utilities.extractTxIdFromRecordValue(record.value(), COL_TXID);
    if (txId != null) {
      LOG.debug("Found transaction id {} in record for table {}", txId, tableName);
      TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
      Map<TopicPartition, Long> partitionTxIds = txIdsByTable.computeIfAbsent(
              tableIdentifier, k -> Maps.newHashMap());
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
          String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
            tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
