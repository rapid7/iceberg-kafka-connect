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
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

public class TransactionDataComplete implements org.apache.iceberg.connect.events.Payload {

    private UUID commitId;
    private List<TopicPartitionOffset> assignments;
    private List<TableTopicPartitionTransaction> tableTxIds;
    private final Schema avroSchema;

    static final int COMMIT_ID = 10_100;
    static final int ASSIGNMENTS = 10_101;
    static final int ASSIGNMENTS_ELEMENT = 10_102;
    static final int TABLE_TX_IDS = 10_201;
    static final int TABLE_TX_IDS_ELEMENT = 10_202;

    private static final Types.StructType ICEBERG_SCHEMA =
            Types.StructType.of(
                    Types.NestedField.required(COMMIT_ID, "commit_id", Types.UUIDType.get()),
                    Types.NestedField.optional(
                            ASSIGNMENTS,
                            "assignments",
                            Types.ListType.ofRequired(ASSIGNMENTS_ELEMENT, TopicPartitionOffset.ICEBERG_SCHEMA)),
                    Types.NestedField.optional(
                            TABLE_TX_IDS,
                            "table_tx_ids",
                            Types.ListType.ofRequired(TABLE_TX_IDS_ELEMENT, TableTopicPartitionTransaction.ICEBERG_SCHEMA)));

    private static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(ICEBERG_SCHEMA,
            TransactionDataComplete.class.getName());

    public TransactionDataComplete(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public TransactionDataComplete(UUID commitId, List<TopicPartitionOffset> assignments, List<TableTopicPartitionTransaction> tableTxIds) {
        this.commitId = commitId;
        this.assignments = assignments;
        this.tableTxIds = tableTxIds;
        this.avroSchema = AVRO_SCHEMA;
    }

    public UUID commitId() {
        return commitId;
    }

    public List<TopicPartitionOffset> assignments() {
        return assignments;
    }

    public List<TableTopicPartitionTransaction> tableTxIds() {
        return tableTxIds;
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public PayloadType type() {
        return org.apache.iceberg.connect.events.PayloadType.DATA_COMPLETE;
    }

    @Override
    public Types.StructType writeSchema() {
        return ICEBERG_SCHEMA;
    }


    @Override
    @SuppressWarnings("unchecked")
    public void put(int i, Object v) {
        switch (positionToId(i, avroSchema)) {
            case COMMIT_ID:
                this.commitId = (UUID) v;
                return;
            case ASSIGNMENTS:
                this.assignments = (List<TopicPartitionOffset>) v;
                return;
            case TABLE_TX_IDS:
                if (v instanceof List) {
                    List<GenericData.Record> records = (List<GenericData.Record>) v;
                    this.tableTxIds = records.stream()
                            .map(TransactionDataComplete::toTableTopicPartitionTransaction)
                            .collect(Collectors.toList());
                }
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (positionToId(i, avroSchema)) {
            case COMMIT_ID:
                return commitId;
            case ASSIGNMENTS:
                return assignments;
            case TABLE_TX_IDS:
                return tableTxIds;
            default:
                throw new UnsupportedOperationException("Unknown field index: " + i);
        }
    }

    static int positionToId(int position, Schema avroSchema) {
        List<Schema.Field> fields = avroSchema.getFields();
        Preconditions.checkArgument(
                position >= 0 && position < fields.size(), "Invalid field position: " + position);
        Object val = fields.get(position).getObjectProp(AvroSchemaUtil.FIELD_ID_PROP);
        return val == null ? -1 : (int) val;
    }

    private static TableTopicPartitionTransaction toTableTopicPartitionTransaction(GenericData.Record record) {
        String topic = record.get("topic").toString();
        int partition = (int) record.get("partition");
        Long txId = (Long) record.get("txId");
        String catalogName = record.get("catalog_name").toString();
        String tableName = record.get("table_name").toString();

        // --- THIS IS THE FIX ---
        // Create a mutable list, add all namespace parts, then add the table name.
        List<String> namespace = ((List<?>) record.get("namespace")).stream()
                .map(Object::toString)
                .collect(Collectors.toList());
        List<String> identifierParts = Lists.newArrayList(namespace);
        identifierParts.add(tableName);

        // Create the identifier from the single, combined array.
        TableIdentifier tableIdentifier = TableIdentifier.of(identifierParts.toArray(new String[0]));
        // --- END FIX ---

        return new TableTopicPartitionTransaction(topic, partition, catalogName, tableIdentifier, txId);
    }
}
