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
package io.tabular.iceberg.connect.data;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.SchemaUpdate.Consumer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergWriter implements RecordWriter {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);
  private static final String COL_TXID = "txid";

  private final Table table;
  private final String tableName;
  private final IcebergSinkConfig config;
  private final List<WriterResult> writerResults;
  private final Map<TopicPartition, Long> partitionMaxTxids = Maps.newHashMap();

  private RecordConverter recordConverter;
  private TaskWriter<Record> writer;

  public IcebergWriter(Table table, String tableName, IcebergSinkConfig config) {
    this.table = table;
    this.tableName = tableName;
    this.config = config;
    this.writerResults = Lists.newArrayList();
    initNewWriter();
  }

  private void initNewWriter() {
    this.writer = Utilities.createTableWriter(table, tableName, config);
    this.recordConverter = new RecordConverter(table, config);
  }

  @Override
  public void write(SinkRecord record) {
    try {
      // TODO: config to handle tombstones instead of always ignoring?
      if (record.value() != null) {
        Long txId = Utilities.extractTxIdFromRecordValue(record.value(), COL_TXID);
        if (txId != null) {
          TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
          partitionMaxTxids.merge(tp, txId, Long::max);
        }
        Record row = convertToRow(record);
        String cdcField = config.tablesCdcField();
        if (cdcField == null) {
          writer.write(row);
        } else {
          LOG.debug("Extracting CDC field: {} from Record {}", cdcField, record.value());
          Operation op = extractCdcOperation(record.value(), cdcField);
          writer.write(new RecordWrapper(row, op));
        }
      }
    } catch (Exception e) {
      throw new DataException(
          String.format(
              "An error occurred converting record, topic: %s, partition, %d, offset: %d",
              record.topic(), record.kafkaPartition(), record.kafkaOffset()),
          e);
    }
  }

  private Record convertToRow(SinkRecord record) {
    if (!config.evolveSchemaEnabled()) {
      return recordConverter.convert(record.value());
    }

    SchemaUpdate.Consumer updates = new Consumer();
    Record row = recordConverter.convert(record.value(), updates);

    if (!updates.empty()) {
      // complete the current file
      flush();
      // apply the schema updates, this will refresh the table
      SchemaUtils.applySchemaUpdates(table, updates);
      LOG.info("Table schema evolution on table {} caused by record at topic: {}, partition: {}, offset: {}", table.name(), record.topic(), record.kafkaPartition(), record.kafkaOffset());
      // initialize a new writer with the new schema
      initNewWriter();
      // convert the row again, this time using the new table schema
      row = recordConverter.convert(record.value(), null);
    }

    return row;
  }

  private Operation extractCdcOperation(Object recordValue, String cdcField) {
    Object opValue = Utilities.extractFromRecordValue(recordValue, cdcField);

    LOG.debug("Processing cdc field value: {}", opValue);
    if (opValue == null) {
      return Operation.INSERT;
    }

    String opStr = opValue.toString().trim().toUpperCase();
    if (opStr.isEmpty()) {
      return Operation.INSERT;
    }

    // TODO: define value mapping in config?

    switch (opStr.charAt(0)) {
      case 'U':
        return Operation.UPDATE;
      case 'D':
        return Operation.DELETE;
      default:
        return Operation.INSERT;
    }
  }

  private void flush() {
    WriteResult writeResult;
    try {
      writeResult = writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    /* Pre-Filtering to align data written events with the filtering logic applied in the coordinator so our
    TransactionDataComplete payload aligns with the data written events */
    long totalRecordCount = 0;
    if (writeResult.dataFiles() != null) {
      totalRecordCount += Arrays.stream(writeResult.dataFiles()).mapToLong(DataFile::recordCount).sum();
    }
    if (writeResult.deleteFiles() != null) {
      totalRecordCount += Arrays.stream(writeResult.deleteFiles()).mapToLong(DeleteFile::recordCount).sum();
    }
    if (totalRecordCount > 0) {
      writerResults.add(
              new WriterResult(
                      TableIdentifier.parse(tableName),
                      Arrays.asList(writeResult.dataFiles()),
                      Arrays.asList(writeResult.deleteFiles()),
                      table.spec().partitionType(),
                      org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.copyOf(partitionMaxTxids)));
    } else if (!partitionMaxTxids.isEmpty()) {
      LOG.debug(" Discarding {} txid entries for a batch that produced no records", partitionMaxTxids.size());
    }
    partitionMaxTxids.clear();
  }

  @Override
  public synchronized List<WriterResult> complete() {
    flush();

    List<WriterResult> result = Lists.newArrayList(writerResults);
    writerResults.clear();

    return result;
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
