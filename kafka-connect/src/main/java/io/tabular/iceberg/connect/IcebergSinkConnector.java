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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import static io.tabular.iceberg.connect.IcebergSinkConfig.INTERNAL_TRANSACTIONAL_SUFFIX_PROP;

public class IcebergSinkConnector extends SinkConnector {

  private Map<String, String> props;

  @Override
  public String version() {
    return IcebergSinkConfig.version();
  }

  public static String getVersion() {
    return IcebergSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return IcebergSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>(maxTasks);

    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>(props);
      config.put(INTERNAL_TRANSACTIONAL_SUFFIX_PROP, UUID.randomUUID().toString());
      configs.add(config);
    }

    return configs;
  }

  @Override
  public void stop() {
    // The IcebergSinkConfig and other resources will be cleaned up by the tasks.
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }
}
