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
package io.tabular.iceberg.connect.transforms;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumTransform.class.getName());

  private static final String CDC_TARGET_PATTERN = "cdc.target.pattern";
  private static final String DB_PLACEHOLDER = "{db}";
  private static final String TABLE_PLACEHOLDER = "{table}";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          CDC_TARGET_PATTERN,
          ConfigDef.Type.STRING,
          null,
          Importance.MEDIUM,
          "Pattern to use for setting the CDC target field value.");

  private String cdcTargetPattern;

  @Override
  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    cdcTargetPattern = config.getString(CDC_TARGET_PATTERN);
  }

  @Override
  public R apply(R record) {
    if (record.value() == null) {
      return record;
    } else if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applyWithSchema(R record) {
    Struct value = Requirements.requireStruct(record.value(), "Debezium transform");

    String op = mapOperation(value.get("op").toString());

    Struct payload;
    Schema payloadSchema;
    if (op.equals(CdcConstants.OP_DELETE)) {
      payload = value.getStruct("before");
      payloadSchema = value.schema().field("before").schema();
    } else {
      payload = value.getStruct("after");
      payloadSchema = value.schema().field("after").schema();
    }

    // create the CDC metadata
    Schema cdcSchema = makeCdcSchema(record.keySchema());
    Struct cdcMetadata = new Struct(cdcSchema);
    cdcMetadata.put(CdcConstants.COL_OP, op);
    cdcMetadata.put(CdcConstants.COL_TS, new java.util.Date(value.getInt64("ts_ms")));
    if (record instanceof SinkRecord) {
      cdcMetadata.put(CdcConstants.COL_OFFSET, ((SinkRecord) record).kafkaOffset());
    }
    setTableAndTargetFromSourceStruct(value.getStruct("source"), cdcMetadata);

    if (record.keySchema() != null) {
      cdcMetadata.put(CdcConstants.COL_KEY, record.key());
    }

    // create the new value
    Schema newValueSchema = makeUpdatedSchema(payloadSchema, cdcSchema);
    if (value.schema().field("ts_ms") != null) {
      newValueSchema = makeUpdatedSchema(newValueSchema, CustomFieldConstants.SOURCE_TIMESTAMP_MS, Timestamp.SCHEMA);
    }

    Struct newValue = new Struct(newValueSchema);

    for (Field field : payloadSchema.fields()) {
      newValue.put(field.name(), payload.get(field));
    }
    newValue.put(CdcConstants.COL_CDC, cdcMetadata);

    if (value.schema().field("ts_ms") != null) {
      newValue.put(CustomFieldConstants.SOURCE_TIMESTAMP_MS, new java.util.Date(value.getInt64("ts_ms")));
    }

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        newValueSchema,
        newValue,
        record.timestamp());
  }

  @SuppressWarnings("unchecked")
  private R applySchemaless(R record) {
    Map<String, Object> value = Requirements.requireMap(record.value(), "Debezium transform");

    String op = mapOperation(value.get("op").toString());

    Object payload;
    if (op.equals(CdcConstants.OP_DELETE)) {
      payload = value.get("before");
    } else {
      payload = value.get("after");
    }

    if (!(payload instanceof Map)) {
      LOG.debug("Unable to transform Debezium record, payload is not a map, skipping");
      return null;
    }

    // create the CDC metadata
    Map<String, Object> cdcMetadata = Maps.newHashMap();
    cdcMetadata.put(CdcConstants.COL_OP, op);
    cdcMetadata.put(CdcConstants.COL_TS, value.get("ts_ms"));
    if (record instanceof SinkRecord) {
      cdcMetadata.put(CdcConstants.COL_OFFSET, ((SinkRecord) record).kafkaOffset());
    }
    setTableAndTargetFromSourceMap(value.get("source"), cdcMetadata);

    if (record.key() instanceof Map) {
      cdcMetadata.put(CdcConstants.COL_KEY, record.key());
    }

    // create the new value
    Map<String, Object> newValue = Maps.newHashMap((Map<String, Object>) payload);
    newValue.put(CdcConstants.COL_CDC, cdcMetadata);

    if (value.containsKey("ts_ms")) {
      long tsms = ((Number) value.get("ts_ms")).longValue();
      newValue.put(CustomFieldConstants.SOURCE_TIMESTAMP_MS, new java.util.Date(tsms));
    }

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        newValue,
        record.timestamp());
  }

  private String mapOperation(String originalOp) {
    switch (originalOp) {
      case "u":
        return CdcConstants.OP_UPDATE;
      case "d":
        return CdcConstants.OP_DELETE;
      default:
        // Debezium ops "c", "r", and any others
        return CdcConstants.OP_INSERT;
    }
  }

  private void setTableAndTargetFromSourceStruct(Struct source, Struct cdcMetadata) {
    String db;

    if (source.schema().field("schema") != null) {
      // prefer schema if present, e.g. for Postgres
      db = source.getString("schema");
    } else {
      db = source.getString("db");
    }

    String table = source.getString("table");

    // Extract transaction ID based on connector type
    Long txid = extractTransactionIdFromSourceStruct(source);

    cdcMetadata.put(CdcConstants.COL_SOURCE, db + "." + table);
    cdcMetadata.put(CdcConstants.COL_TARGET, target(db, table));
    cdcMetadata.put(CdcConstants.COL_TXID, txid);
  }

  private Long extractTransactionIdFromSourceStruct(Struct source) {
    String connector = source.getString("connector");

    if ("postgresql".equals(connector)) {
      // Check for txId field for postgresql
      if (source.schema().field("txId") != null) {
        return source.getInt64("txId");
      }
    } else if ("mysql".equals(connector)) {
      // Check if in snapshot mode first
      if (source.schema().field("snapshot") != null && !"false".equals(source.getString("snapshot"))) {
        // Return 1 as gtid is null when snapshotting
        return 1L;
      }

      // Not in snapshot mode, check for gtid field for mysql
      if (source.schema().field("gtid") != null) {
        String gtid = source.getString("gtid");
        // Split gtid into uuid and txid
        String[] gtidSections = gtid.split(":");
        // Return txid section
        if (gtidSections.length == 2) {
          return Long.valueOf(gtidSections[1]);
        }
      }
    } else {
      LOG.warn("Transactional consistency is not currently supported for connector type: {}", connector);
    }
    return null;
  }

  private void setTableAndTargetFromSourceMap(Object source, Map<String, Object> cdcMetadata) {
    Map<String, Object> map = Requirements.requireMap(source, "Debezium transform");

    String db;
    if (map.containsKey("schema")) {
      // prefer schema if present, e.g. for Postgres
      db = map.get("schema").toString();
    } else {
      db = map.get("db").toString();
    }
    String table = map.get("table").toString();

    // Extract transaction ID based on connector type
    Long txid = extractTransactionIdFromSourceMap(map);

    cdcMetadata.put(CdcConstants.COL_SOURCE, db + "." + table);
    cdcMetadata.put(CdcConstants.COL_TARGET, target(db, table));
    cdcMetadata.put(CdcConstants.COL_TXID, txid);
  }

  private Long extractTransactionIdFromSourceMap(Map<String, Object> source) {
    String connector = source.get("connector").toString();

    if ("postgresql".equals(connector)) {
      // Check for txId field for postgresql
      if (source.containsKey("txId")) {
        // Return txid
        return Long.valueOf(source.get("txId").toString());
      }
    } else if ("mysql".equals(connector)) {
      // Check if in snapshot mode first
      if (source.containsKey("snapshot") && !"false".equals(source.get("snapshot").toString())) {
        // Return 1 as gtid is null when snapshotting
        return 1L;
      }

      // Not in snapshot mode, check for gtid field for mysql
      if (source.containsKey("gtid")) {
        String gtid = source.get("gtid").toString();
        // Split gtid into uuid and txid
        String[] gtidSections = gtid.split(":");
        // Return txid section
        if (gtidSections.length == 2) {
          return Long.valueOf(gtidSections[1]);
        }
      }
    } else {
      LOG.warn("Transactional consistency is not currently supported for connector type: {}", connector);
    }
    return null;
  }

  private String target(String db, String table) {
    return cdcTargetPattern == null || cdcTargetPattern.isEmpty()
        ? db + "." + table
        : cdcTargetPattern.replace(DB_PLACEHOLDER, db).replace(TABLE_PLACEHOLDER, table);
  }

  private Schema makeCdcSchema(Schema keySchema) {
    SchemaBuilder builder = SchemaBuilder.struct()
        .field(CdcConstants.COL_OP, Schema.STRING_SCHEMA)
        .field(CdcConstants.COL_TS, Timestamp.SCHEMA)
        .field(CdcConstants.COL_OFFSET, Schema.OPTIONAL_INT64_SCHEMA)
        .field(CdcConstants.COL_SOURCE, Schema.STRING_SCHEMA)
        .field(CdcConstants.COL_TARGET, Schema.STRING_SCHEMA)
        .field(CdcConstants.COL_TXID, Schema.OPTIONAL_INT64_SCHEMA);

    if (keySchema != null) {
      builder.field(CdcConstants.COL_KEY, keySchema);
    }

    return builder.build();
  }

  private Schema makeUpdatedSchema(Schema schema, String fieldName, Schema fieldSchema) {
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field(fieldName, fieldSchema);

    return builder.build();
  }

  private Schema makeUpdatedSchema(Schema schema, Schema cdcSchema) {
    return makeUpdatedSchema(schema, CdcConstants.COL_CDC, cdcSchema);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }
}
