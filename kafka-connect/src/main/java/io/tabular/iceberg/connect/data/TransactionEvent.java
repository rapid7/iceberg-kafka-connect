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

import org.apache.avro.Schema;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class TransactionEvent extends Event {

     private Map<TopicPartition, Long> txIdPerPartition = Maps.newHashMap();

    public TransactionEvent(Schema schema) {
        super(schema);
    }

    public TransactionEvent(String groupId, Payload payload) {
        super(groupId, payload);
    }

    public TransactionEvent(String groupId, Payload payload, Map<TopicPartition, Long> txIdPerPartition) {
        super(groupId, payload);
        this.txIdPerPartition = txIdPerPartition;
    }

    public Map<TopicPartition, Long> txIdPerPartition() {
        return txIdPerPartition;
    }
}
