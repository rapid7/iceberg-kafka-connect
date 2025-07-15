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
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

public class TransactionDataComplete implements org.apache.iceberg.connect.events.Payload {

    private UUID commitId;
    private List<TopicPartitionOffset> assignments;
    private final Schema avroSchema;

    static final int COMMIT_ID = 10_100;
    static final int ASSIGNMENTS = 10_101;
    static final int ASSIGNMENTS_ELEMENT = 10_102;

    private static final Types.StructType ICEBERG_SCHEMA =
            Types.StructType.of(
                    Types.NestedField.required(COMMIT_ID, "commit_id", Types.UUIDType.get()),
                    Types.NestedField.optional(
                            ASSIGNMENTS,
                            "assignments",
                            Types.ListType.ofRequired(ASSIGNMENTS_ELEMENT, TopicPartitionOffset.ICEBERG_SCHEMA)));

    private static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(ICEBERG_SCHEMA,
            TransactionDataComplete.class.getName());

    public TransactionDataComplete(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public TransactionDataComplete(UUID commitId, List<TopicPartitionOffset> assignments) {
        this.commitId = commitId;
        this.assignments = assignments;
        this.avroSchema = AVRO_SCHEMA;
    }

    public UUID commitId() {
        return commitId;
    }

    public List<TopicPartitionOffset> assignments() {
        return assignments;
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
            default:
        }
    }

    @Override
    public Object get(int i) {
        switch (positionToId(i, avroSchema)) {
            case COMMIT_ID:
                return commitId;
            case ASSIGNMENTS:
                return assignments;
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
}
