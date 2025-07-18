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

import java.util.List;
import java.util.Map;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.common.TopicPartition;

public class WriterResult {

  private final TableIdentifier tableIdentifier;
  private final List<DataFile> dataFiles;
  private final List<DeleteFile> deleteFiles;
  private final StructType partitionStruct;
  private final Map<TopicPartition, Long> partitionMaxTxids;

  public WriterResult(
      TableIdentifier tableIdentifier,
      List<DataFile> dataFiles,
      List<DeleteFile> deleteFiles,
      StructType partitionStruct,
      Map<TopicPartition, Long> partitionMaxTxids) {
    this.tableIdentifier = tableIdentifier;
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;
    this.partitionStruct = partitionStruct;
    this.partitionMaxTxids = partitionMaxTxids;
  }

  public TableIdentifier tableIdentifier() {
    return tableIdentifier;
  }

  public List<DataFile> dataFiles() {
    return dataFiles;
  }

  public List<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  public StructType partitionStruct() {
    return partitionStruct;
  }

  public Map<TopicPartition, Long> partitionMaxTxids() {
    return partitionMaxTxids;
  }
}
