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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;

public class DebeziumTransformTest {

  private static final long TS_MS = System.currentTimeMillis();
  private static final long TS_US = TS_MS * 1000;

  private static final Schema KEY_SCHEMA = SchemaBuilder.struct().field("account_id", Schema.INT64_SCHEMA).build();

  private static final Schema ROW_SCHEMA = SchemaBuilder.struct()
      .field("account_id", Schema.INT64_SCHEMA)
      .field("balance", Decimal.schema(2))
      .field("last_updated", Schema.STRING_SCHEMA)
      .build();

  private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
      .field("db", Schema.STRING_SCHEMA)
      .field("schema", Schema.STRING_SCHEMA)
      .field("table", Schema.STRING_SCHEMA)
      .field("connector", Schema.STRING_SCHEMA)
      .field("snapshot", Schema.STRING_SCHEMA)
      .field("txId", Schema.OPTIONAL_INT64_SCHEMA)
      .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ts_ms", Schema.INT64_SCHEMA)
      .field("ts_us", Schema.INT64_SCHEMA)
      .build();

  // Original VALUE_SCHEMA
  private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
      .field("op", Schema.STRING_SCHEMA)
      .field("ts_ms", Schema.INT64_SCHEMA)
      .field("ts_us", Schema.INT64_SCHEMA)
      .field("source", SOURCE_SCHEMA)
      .field("before", ROW_SCHEMA)
      .field("after", ROW_SCHEMA)
      .build();

  @Test
  public void testDmsTransformNull() {
    try (DmsTransform<SinkRecord> smt = new DmsTransform<>()) {
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isNull();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDebeziumTransformSchemaless() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Map<String, Object> event = createDebeziumEventMap("u", "postgresql", "false", 1L, null);
      Map<String, Object> key = ImmutableMap.of("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, null, key, null, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Map.class);
      Map<String, Object> value = (Map<String, Object>) result.value();

      assertThat(value.get("account_id")).isEqualTo(1);

      Map<String, Object> cdcMetadata = (Map<String, Object>) value.get("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Map.class);

      // Verify txid has been added
      assertThat(cdcMetadata.get("txid")).isEqualTo(1L);

      // Verify source_ts_ms has been added from source.ts_ms
      Map<String, Object> sourceMap = (Map<String, Object>) ((Map<String, Object>) event.get("source"));
      assertThat(value.get("source_ts_ms")).isEqualTo(sourceMap.get("ts_ms"));

      assertThat(value.get("source_ts_ms")).isNotEqualTo(new java.util.Date(TS_MS));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDebeziumTransformSchemalessMySQL() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Map<String, Object> event = createDebeziumEventMap("u", "mysql", "false", null, "0000-0000-0000:1");
      Map<String, Object> key = ImmutableMap.of("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, null, key, null, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Map.class);
      Map<String, Object> value = (Map<String, Object>) result.value();

      assertThat(value.get("account_id")).isEqualTo(1);

      Map<String, Object> cdcMetadata = (Map<String, Object>) value.get("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Map.class);

      // Verify txid has been extracted from gtid
      assertThat(cdcMetadata.get("txid")).isEqualTo(1L);

      // Verify source_ts_ms has been added from source.ts_ms
      Map<String, Object> sourceMap = (Map<String, Object>) ((Map<String, Object>) event.get("source"));
      assertThat(value.get("source_ts_ms")).isEqualTo(sourceMap.get("ts_ms"));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDebeziumTransformSchemalessMySQLSnapshotting() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Map<String, Object> event = createDebeziumEventMap("u", "mysql", "true", null, null);
      Map<String, Object> key = ImmutableMap.of("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, null, key, null, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Map.class);
      Map<String, Object> value = (Map<String, Object>) result.value();

      assertThat(value.get("account_id")).isEqualTo(1);

      Map<String, Object> cdcMetadata = (Map<String, Object>) value.get("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Map.class);

      // Verify txid has been set to 1 as gtid is null when snapshotting
      assertThat(cdcMetadata.get("txid")).isEqualTo(1L);

       // Verify source_ts_ms has been added from source.ts_ms
      Map<String, Object> sourceMap = (Map<String, Object>) ((Map<String, Object>) event.get("source"));
      System.err.println(value.get("source_ts_ms"));
      System.err.println(sourceMap.get("ts_ms"));
      assertThat(value.get("source_ts_ms")).isEqualTo(sourceMap.get("ts_ms"));
    }
  }

  @Test
  public void testDebeziumTransformWithSchema() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Struct event = createDebeziumEventStruct("u", "postgresql", "false", 1L, null);
      Struct key = new Struct(KEY_SCHEMA).put("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, KEY_SCHEMA, key, VALUE_SCHEMA, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();

      assertThat(value.get("account_id")).isEqualTo(1L);

      Struct cdcMetadata = value.getStruct("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Struct.class);

      // Verify txid has been added
      assertThat(cdcMetadata.get("txid")).isEqualTo(1L);

      // Verify source_ts_ms is added and correct from source.ts_ms
      Struct sourceStruct = event.getStruct("source");
      Schema tsmsSchema = value.schema().field("source_ts_ms").schema();
      assertThat(tsmsSchema).isEqualTo(Timestamp.SCHEMA);
      assertThat(value.get("source_ts_ms")).isEqualTo(new java.util.Date(sourceStruct.getInt64("ts_ms")));

      assertThat(value.get("source_ts_ms")).isNotEqualTo(event.getInt64("ts_ms"));
    }
  }

  @Test
  public void testDebeziumTransformWithSchemaMySQL() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Struct event = createDebeziumEventStruct("u", "mysql", "false", null, "0000-0000-0000:1");
      Struct key = new Struct(KEY_SCHEMA).put("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, KEY_SCHEMA, key, VALUE_SCHEMA, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();

      assertThat(value.get("account_id")).isEqualTo(1L);

      Struct cdcMetadata = value.getStruct("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Struct.class);

      // Verify txid has been extracted from gtid
      assertThat(cdcMetadata.get("txid")).isEqualTo(1L);

      // Verify source_ts_ms is added and correct from source.ts_ms
      Struct sourceStruct = event.getStruct("source");
      Schema tsmsSchema = value.schema().field("source_ts_ms").schema();
      assertThat(tsmsSchema).isEqualTo(Timestamp.SCHEMA);
      assertThat(value.get("source_ts_ms")).isEqualTo(new java.util.Date(sourceStruct.getInt64("ts_ms")));
    }
  }

  @Test
  public void testDebeziumTransformWithSchemaMySQLSnapshotting() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Struct event = createDebeziumEventStruct("u", "mysql", "first_in_data_collection", null, null);
      Struct key = new Struct(KEY_SCHEMA).put("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, KEY_SCHEMA, key, VALUE_SCHEMA, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();

      assertThat(value.get("account_id")).isEqualTo(1L);

      Struct cdcMetadata = value.getStruct("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Struct.class);

      // Verify txid has been set to 1 as gtid is null when snapshotting
      assertThat(cdcMetadata.get("txid")).isEqualTo(1L);

      // Verify source_ts_ms is added and correct from source.ts_ms
      Struct sourceStruct = event.getStruct("source");
      Schema tsmsSchema = value.schema().field("source_ts_ms").schema();
      assertThat(tsmsSchema).isEqualTo(Timestamp.SCHEMA);
      assertThat(value.get("source_ts_ms")).isEqualTo(new java.util.Date(sourceStruct.getInt64("ts_ms")));
    }
  }

  private Map<String, Object> createDebeziumEventMap(String operation, String connector, String snapshot, Long txid,
      String gtid) {
    Map<String, Object> source = Maps.newHashMap();
    source.put("db", "db");
    source.put("schema", "schema");
    source.put("table", "tbl");
    source.put("connector", connector);
    source.put("snapshot", snapshot);
    source.put("txId", txid);
    source.put("gtid", gtid);
    source.put("ts_ms", TS_MS);
    source.put("ts_us", TS_US);

    Map<String, Object> data = ImmutableMap.of(
        "account_id", 1,
        "balance", 100,
        "last_updated", Instant.now().toString());

    return ImmutableMap.of(
        "op", operation,
        "ts_ms", TS_MS+1,
        "ts_us", TS_US+1,
        "source", source,
        "before", data,
        "after", data);
  }

  private Struct createDebeziumEventStruct(String operation, String connector, String snapshot, Long txid,
      String gtid) {
    Struct source = new Struct(SOURCE_SCHEMA)
        .put("db", "db")
        .put("schema", "schema")
        .put("table", "tbl")
        .put("connector", connector)
        .put("snapshot", snapshot)
        .put("txId", txid)
        .put("gtid", gtid)
        .put("ts_ms", TS_MS)
        .put("ts_us", TS_US);

    Struct data = new Struct(ROW_SCHEMA)
        .put("account_id", 1L)
        .put("balance", BigDecimal.valueOf(100))
        .put("last_updated", Instant.now().toString());

    return new Struct(VALUE_SCHEMA)
        .put("op", operation)
        .put("ts_ms", TS_MS+1)
        .put("ts_us", TS_US+1)
        .put("source", source)
        .put("before", data)
        .put("after", data);
  }

}
