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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.UUID;

public class DataWrittenTxId implements org.apache.iceberg.connect.events.Payload{
    private Types.StructType partitionType;

    private UUID commitId;
    private TableReference tableReference;
    private List<DataFile> dataFiles;
    private List<DeleteFile> deleteFiles;
    private TopicPartitionTransaction topicPartitionTransaction;
    private Types.StructType icebergSchema;
    private final Schema avroSchema;

    static final int COMMIT_ID = 10_300;
    static final int TABLE_REFERENCE = 10_301;
    static final int DATA_FILES = 10_302;
    static final int DATA_FILES_ELEMENT = 10_303;
    static final int DELETE_FILES = 10_304;
    static final int DELETE_FILES_ELEMENT = 10_305;
    static final int TOPIC_PARTITION_TRANSACTION = 10_306;

    public DataWrittenTxId(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public DataWrittenTxId(
            Types.StructType partitionType,
            UUID commitId,
            TableReference tableReference,
            List<DataFile> dataFiles,
            List<DeleteFile> deleteFiles,
            TopicPartitionTransaction topicPartitionTransaction) {
        Preconditions.checkNotNull(commitId, "Commit ID cannot be null");
        Preconditions.checkNotNull(tableReference, "Table reference cannot be null");
        this.partitionType = partitionType;
        this.commitId = commitId;
        this.tableReference = tableReference;
        this.dataFiles = dataFiles;
        this.deleteFiles = deleteFiles;
        this.topicPartitionTransaction = topicPartitionTransaction;
        this.avroSchema = AvroSchemaUtil.convert(writeSchema(), getClass().getName());
    }


    @Override
    public PayloadType type() {
        return PayloadType.DATA_WRITTEN;
    }

    public UUID commitId() {
        return commitId;
    }

    public TableReference tableReference() {
        return tableReference;
    }

    public List<DataFile> dataFiles() {
        return dataFiles;
    }

    public List<DeleteFile> deleteFiles() {
        return deleteFiles;
    }

    public TopicPartitionTransaction topicPartitionTransaction() {
        return topicPartitionTransaction;
    }

    @Override
    public Types.StructType writeSchema() {
        if (icebergSchema == null) {
            Types.StructType dataFileStruct = DataFile.getType(partitionType);

            this.icebergSchema =
                    Types.StructType.of(
                            Types.NestedField.required(COMMIT_ID, "commit_id", Types.UUIDType.get()),
                            Types.NestedField.required(
                                    TABLE_REFERENCE, "table_reference", TableReference.ICEBERG_SCHEMA),
                            Types.NestedField.optional(
                                    DATA_FILES,
                                    "data_files",
                                    Types.ListType.ofRequired(DATA_FILES_ELEMENT, dataFileStruct)),
                            Types.NestedField.optional(
                                    DELETE_FILES,
                                    "delete_files",
                                    Types.ListType.ofRequired(DELETE_FILES_ELEMENT, dataFileStruct)),
                            Types.NestedField.optional(
                                    TOPIC_PARTITION_TRANSACTION,
                                    "topic_partition_transaction", TopicPartitionTransaction.ICEBERG_SCHEMA)
                            );
        }

        return icebergSchema;
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void put(int i, Object v) {
        switch (positionToId(i, avroSchema)) {
            case COMMIT_ID:
                this.commitId = (UUID) v;
                return;
            case TABLE_REFERENCE:
                this.tableReference = (TableReference) v;
                return;
            case DATA_FILES:
                this.dataFiles = (List<DataFile>) v;
                return;
            case DELETE_FILES:
                this.deleteFiles = (List<DeleteFile>) v;
                return;
            case TOPIC_PARTITION_TRANSACTION:
                this.topicPartitionTransaction = convertToTopicPartitionTransaction((GenericData.Record) v);
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
            case TABLE_REFERENCE:
                return tableReference;
            case DATA_FILES:
                return dataFiles;
            case DELETE_FILES:
                return deleteFiles;
            case TOPIC_PARTITION_TRANSACTION:
                return topicPartitionTransaction;
            default:
                throw new UnsupportedOperationException("Unknown field ordinal: " + i);
        }
    }

    static int positionToId(int position, Schema avroSchema) {
        List<Schema.Field> fields = avroSchema.getFields();
        Preconditions.checkArgument(
                position >= 0 && position < fields.size(), "Invalid field position: " + position);
        Object val = fields.get(position).getObjectProp(AvroSchemaUtil.FIELD_ID_PROP);
        return val == null ? -1 : (int) val;
    }

    public TopicPartitionTransaction convertToTopicPartitionTransaction(GenericData.Record record) {
        if (record == null) {return null;}
        String topic = record.get("topic").toString() ;
        Integer partition = (Integer) record.get("partition");
        Long transactionId = (Long) record.get("txId");
        return new TopicPartitionTransaction(topic, partition, transactionId);
    }
}
