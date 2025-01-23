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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;

public class DebeziumTransformTest {

  private static final Schema KEY_SCHEMA =
      SchemaBuilder.struct().field("account_id", Schema.INT64_SCHEMA).build();

  private static final Schema ROW_SCHEMA =
      SchemaBuilder.struct()
          .field("account_id", Schema.INT64_SCHEMA)
          .field("balance", Decimal.schema(2))
          .field("last_updated", Schema.STRING_SCHEMA)
          .build();

  private static final Schema SOURCE_SCHEMA =
      SchemaBuilder.struct()
          .field("db", Schema.STRING_SCHEMA)
          .field("schema", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .build();
  
  // Original VALUE_SCHEMA 
  private static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .field("op", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.INT64_SCHEMA)
          .field("source", SOURCE_SCHEMA)
          .field("before", ROW_SCHEMA)
          .field("after", ROW_SCHEMA)
          .field("txid", Schema.INT64_SCHEMA)
          .build();

  // New VALUE_SCHEMA with ts_us to test forked changes
  private static final Schema VALUE_SCHEMA_WITH_TS_US =
      SchemaBuilder.struct()
          .field("op", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.INT64_SCHEMA)
          .field("ts_us", Schema.INT64_SCHEMA)
          .field("source", SOURCE_SCHEMA)
          .field("before", ROW_SCHEMA)
          .field("after", ROW_SCHEMA)
          .field("txid", Schema.INT64_SCHEMA)
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

      Map<String, Object> event = createDebeziumEventMap("u");
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

      // Verify source_ts_us has not been added
      assertThat(value.containsKey("source_ts_us")).isFalse();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDebeziumTransformSchemalessWithTsUs() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Map<String, Object> event = createDebeziumEventMap("u");
      // Add ts_us to the event
      long ts_us = System.currentTimeMillis() * 1000;
      event = new ImmutableMap.Builder<String, Object>()
          .putAll(event)
          .put("ts_us", ts_us)
          .build();

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

      // Verify source_ts_us has been added and is correct
      assertThat(value.get("source_ts_us")).isEqualTo(ts_us);
    }
  }

  @Test
  public void testDebeziumTransformWithSchema() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));
      
      Struct event = createDebeziumEventStruct("u");
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

      // Verify source_ts_us has not been added
      assertThat(value.schema().field("source_ts_us")).isNull();
    }
  }

  @Test
  public void testDebeziumTransformWithSchemAndTsUs() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      long ts_us = System.currentTimeMillis() * 1000L;
      Struct event = createDebeziumEventStructWithTsUs("u", ts_us);
      Struct key = new Struct(KEY_SCHEMA).put("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, KEY_SCHEMA, key, VALUE_SCHEMA_WITH_TS_US, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();

      assertThat(value.get("account_id")).isEqualTo(1L);

      Struct cdcMetadata = value.getStruct("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Struct.class);

      // Verify source_ts_us is added and correct
      Schema tsUsSchema = value.schema().field("source_ts_us").schema();
      assertThat(tsUsSchema).isEqualTo(Timestamp.SCHEMA);
      assertThat(value.get("source_ts_us")).isEqualTo(new java.util.Date(ts_us / 1000L));
    }
  }

  private Map<String, Object> createDebeziumEventMap(String operation) {
    Map<String, Object> source =
        ImmutableMap.of(
            "db", "db",
            "schema", "schema",
            "table", "tbl");

    Map<String, Object> data =
        ImmutableMap.of(
            "account_id", 1,
            "balance", 100,
            "last_updated", Instant.now().toString());

    return ImmutableMap.of(
        "op", operation,
        "ts_ms", System.currentTimeMillis(),
        "source", source,
        "before", data,
        "after", data);
  }

  private Struct createDebeziumEventStruct(String operation) {
    Struct source =
        new Struct(SOURCE_SCHEMA).put("db", "db").put("schema", "schema").put("table", "tbl");

    Struct data =
        new Struct(ROW_SCHEMA)
            .put("account_id", 1L)
            .put("balance", BigDecimal.valueOf(100))
            .put("last_updated", Instant.now().toString());

    return new Struct(VALUE_SCHEMA)
        .put("op", operation)
        .put("ts_ms", System.currentTimeMillis())
        .put("source", source)
        .put("before", data)
        .put("after", data);
  }

  private Struct createDebeziumEventStructWithTsUs(String operation, Long ts_us) {
    Struct source =
        new Struct(SOURCE_SCHEMA).put("db", "db").put("schema", "schema").put("table", "tbl");

    Struct data =
        new Struct(ROW_SCHEMA)
            .put("account_id", 1L)
            .put("balance", BigDecimal.valueOf(100))
            .put("last_updated", Instant.now().toString());

    return new Struct(VALUE_SCHEMA_WITH_TS_US)
        .put("op", operation)
        .put("ts_ms", System.currentTimeMillis())
        .put("ts_us", ts_us)
        .put("source", source)
        .put("before", data)
        .put("after", data);
  }
}
