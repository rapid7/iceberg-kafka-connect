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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.Utilities;
import java.util.Collection;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskImpl implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(TaskImpl.class);

  private final IcebergSinkConfig config;
  private final Catalog catalog;
  private final Worker writer;
  private final Committer committer;

  public TaskImpl(SinkTaskContext context, IcebergSinkConfig config) {
    this.config = config;
    this.catalog = Utilities.loadCatalog(this.config);
    this.writer = new Worker(this.config, catalog);
    this.committer = new CommitterImpl(context, this.config, catalog, this.writer);
  }

  @Override
  public String version() {
    return IcebergSinkConfig.version();
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    writer.write(sinkRecords);
  }

  @Override
  public void commit() {
    LOG.debug("Commit called on TaskImpl");
    committer.process();
    committer.commit(writer::committable);
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing TaskImpl");
    Utilities.close(committer);
    Utilities.close(writer);
    Utilities.close(catalog);
  }
}
