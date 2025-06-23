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
package io.tabular.iceberg.connect.events;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

public class TableTopicPartitionTransaction implements org.apache.avro.generic.IndexedRecord {

    private String topic;
    private Integer partition;
    private String catalogName;
    private List<String> namespace;
    private String tableName;
    private Long txId;

    private final Schema avroSchema;

    static final int TOPIC = 10_800;
    static final int PARTITION = 10_801;
    static final int TX_ID = 10_802;
    static final int CATALOG_NAME = 10_803;
    static final int NAMESPACE = 10_804;
    static final int TABLE_NAME = 10_805;
    static final int NAMESPACE_ELEMENT = 10_806;

    public static final Types.StructType ICEBERG_SCHEMA =
            Types.StructType.of(
                    Types.NestedField.required(TOPIC, "topic", Types.StringType.get()),
                    Types.NestedField.required(PARTITION, "partition", Types.IntegerType.get()),
                    Types.NestedField.optional(TX_ID, "txId", Types.LongType.get()),
                    Types.NestedField.required(CATALOG_NAME, "catalog_name", Types.StringType.get()),
                    Types.NestedField.required(NAMESPACE, "namespace", Types.ListType.ofRequired(NAMESPACE_ELEMENT, Types.StringType.get())),
                    Types.NestedField.required(TABLE_NAME, "table_name", Types.StringType.get())
            );

    private static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(ICEBERG_SCHEMA, TableTopicPartitionTransaction.class.getName());

    public TableTopicPartitionTransaction(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public TableTopicPartitionTransaction(String topic, int partition, String catalogName, TableIdentifier tableIdentifier, Long txId) {
        this.topic = topic;
        this.partition = partition;
        this.catalogName = catalogName;
        this.namespace = Lists.newArrayList(tableIdentifier.namespace().levels());
        this.tableName = tableIdentifier.name();
        this.txId = txId;
        this.avroSchema = AVRO_SCHEMA;
    }

    public String topic() { return topic; }
    public Integer partition() { return partition; }
    public String catalogName() { return catalogName; }
    public Long txId() { return txId; }

    // --- THIS IS THE FIX ---
    public TableIdentifier tableIdentifier() {
        List<String> parts = Lists.newArrayList(namespace);
        parts.add(tableName);
        return TableIdentifier.of(parts.toArray(new String[0]));
    }
    // --- END FIX ---

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void put(int i, Object v) {
        switch (positionToId(i, avroSchema)) {
            case TOPIC:
                this.topic = v == null ? null : v.toString();
                return;
            case PARTITION:
                this.partition = (Integer) v;
                return;
            case TX_ID:
                this.txId = (Long) v;
                return;
            case CATALOG_NAME:
                this.catalogName = v == null ? null : v.toString();
                return;
            case NAMESPACE:
                if (v instanceof List) {
                    this.namespace = ((List<?>) v).stream()
                            .map(Object::toString)
                            .collect(Collectors.toList());
                }
                return;
            case TABLE_NAME:
                this.tableName = v == null ? null : v.toString();
                return;
            default:
                throw new IllegalArgumentException("Unknown field index: " + i);
        }
    }

    @Override
    public Object get(int i) {
        switch (positionToId(i, avroSchema)) {
            case TOPIC: return topic;
            case PARTITION: return partition;
            case TX_ID: return txId;
            case CATALOG_NAME: return catalogName;
            case NAMESPACE: return namespace;
            case TABLE_NAME: return tableName;
            default:
                throw new IllegalArgumentException("Unknown field index: " + i);
        }
    }

    static int positionToId(int position, Schema avroSchema) {
        List<Schema.Field> fields = avroSchema.getFields();
        Preconditions.checkArgument(
                position >= 0 && position < fields.size(), "Invalid field position: " + position);
        Object val = fields.get(position).getObjectProp(AvroSchemaUtil.FIELD_ID_PROP);
        return val == null ? -1 : (int) val;
    }
}
