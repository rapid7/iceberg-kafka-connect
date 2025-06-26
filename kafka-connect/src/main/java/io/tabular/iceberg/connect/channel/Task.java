/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.channel;

import java.util.Collection;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A public interface for the sink task's main logic, encapsulating the writer and committer. This
 * allows the IcebergSinkTask to be in a different package and still access the necessary methods.
 */
public interface Task extends AutoCloseable {

  /**
   * Returns the version of the connector.
   *
   * @return the version, formatted as a string
   */
  String version();

  /**
   * Delivers a collection of records to be written to Iceberg tables.
   *
   * @param sinkRecords the records to write
   */
  void put(Collection<SinkRecord> sinkRecords);

  /**
   * Orchestrates the commit process for data that has been written. This should be called from the
   * SinkTask's preCommit/flush method.
   */
  void commit();

  /**
   * Closes all resources, including the writer, committer, and catalog.
   *
   * @throws Exception if an error occurs during closing
   */
  @Override
  void close() throws Exception;
}
