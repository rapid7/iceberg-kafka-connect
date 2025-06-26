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
package io.tabular.iceberg.connect;

import io.tabular.iceberg.connect.channel.Task;
import io.tabular.iceberg.connect.channel.TaskImpl;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkTask.class);

  private IcebergSinkConfig config;
  private Task task;

  @Override
  public String version() {
    return IcebergSinkConnector.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    LOG.info("Starting IcebergSinkTask");
    this.config = new IcebergSinkConfig(props);
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    LOG.info("Opening IcebergSinkTask for partitions: {}", partitions);
    // The TaskImpl will encapsulate the worker and committer, resolving visibility issues.
    this.task = new TaskImpl(context, config);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    LOG.info("Closing IcebergSinkTask for partitions: {}", partitions);
    // The stop method will handle the cleanup.
    stop();
  }

  @Override
  public void stop() {
    LOG.info("Stopping IcebergSinkTask");
    if (task != null) {
      try {
        task.close();
      } catch (Exception e) {
        LOG.warn("An error occurred closing the task", e);
      }
      task = null;
    }
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    if (task != null) {
      task.put(sinkRecords);
    } else {
      LOG.warn("Task is not initialized, skipping {} records", sinkRecords.size());
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
          Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    LOG.debug("preCommit called, triggering commit phase");
    try {
      if (task != null) {
        // Delegate the commit logic to the task implementation.
        task.commit();
      } else {
        LOG.warn("Task is not initialized, skipping commit");
      }
    } catch (Exception e) {
      LOG.error("Commit failed, the task will be restarted.", e);
      // Throwing an exception here will cause the framework to retry and eventually
      // restart the task if the error is persistent.
      throw new ConnectException("Commit failed, see logs for details", e);
    }

    // We manage our own offsets, so we don't return anything for the framework to commit.
    return Collections.emptyMap();
  }
}
