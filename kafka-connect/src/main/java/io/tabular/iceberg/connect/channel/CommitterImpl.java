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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.events.TableTopicPartitionTransaction;
import io.tabular.iceberg.connect.events.TransactionDataComplete;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitterImpl extends Channel implements Committer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);
  private final SinkTaskContext context;
  private final IcebergSinkConfig config;
  private final Optional<CoordinatorThread> maybeCoordinatorThread;
  private final Worker worker;

  public CommitterImpl(SinkTaskContext context, IcebergSinkConfig config, Catalog catalog, Worker worker) {
    this(
            context,
            config,
            new KafkaClientFactory(config.kafkaProps()),
            new CoordinatorThreadFactoryImpl(catalog, new KafkaClientFactory(config.kafkaProps())),
            worker);
  }

  @VisibleForTesting
  CommitterImpl(
          SinkTaskContext context,
          IcebergSinkConfig config,
          KafkaClientFactory clientFactory,
          CoordinatorThreadFactory coordinatorThreadFactory,
          Worker worker) {
    super(
            "committer",
            IcebergSinkConfig.DEFAULT_CONTROL_GROUP_PREFIX + UUID.randomUUID(),
            config,
            clientFactory);

    this.context = context;
    this.config = config;
    this.worker = worker;
    this.maybeCoordinatorThread = coordinatorThreadFactory.create(context, config);

    Map<TopicPartition, Long> stableConsumerOffsets =
            fetchStableConsumerOffsets(config.controlGroupId());
    context.offset(stableConsumerOffsets);

    // This initial poll is for initialization, which is fine.
    consumeAvailable(Duration.ofMillis(1000), envelope -> receive(envelope, this.worker));
  }


  private Map<TopicPartition, Long> fetchStableConsumerOffsets(String groupId) {
    try {
      ListConsumerGroupOffsetsResult response =
              admin()
                      .listConsumerGroupOffsets(
                              groupId, new ListConsumerGroupOffsetsOptions().requireStable(true));
      return response.partitionsToOffsetAndMetadata().get().entrySet().stream()
              .filter(entry -> context.assignment().contains(entry.getKey()))
              .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
    } catch (InterruptedException | ExecutionException e) {
      throw new ConnectException(e);
    }
  }

  private void throwExceptionIfCoordinatorIsTerminated() {
    if (maybeCoordinatorThread.map(CoordinatorThread::isTerminated).orElse(false)) {
      throw new IllegalStateException("Coordinator unexpectedly terminated");
    }
  }

  private boolean receive(Envelope envelope, CommittableSupplier committableSupplier) {
    if (envelope.event().type() == PayloadType.START_COMMIT) {
      LOG.info("Received START_COMMIT event");
      UUID commitId = ((StartCommit) envelope.event().payload()).commitId();
      sendCommitResponse(commitId, committableSupplier);
      return true;
    }
    return false;
  }

  private void sendCommitResponse(UUID commitId, CommittableSupplier committableSupplier) {
    Committable committable = committableSupplier.committable(commitId);
    LOG.info("Sending commit {} to committable {}", commitId, committable);
    if (committable == null) {
      LOG.warn("Committable was null for commit ID {}, skipping response", commitId);
      return;
    }

    List<Event> events = Lists.newArrayList();

    // Handle writer results
    committable.writerResults().forEach(writerResult -> {
      Event commitResponse = new Event(
              config.controlGroupId(),
              new DataWritten(
                      writerResult.partitionStruct(),
                      commitId,
                      TableReference.of(config.catalogName(), writerResult.tableIdentifier()),
                      writerResult.dataFiles(),
                      writerResult.deleteFiles()));

      events.add(commitResponse);
    });

    List<TopicPartitionOffset> assignments = context.assignment().stream()
            .map(topicPartition -> {
              Offset offset = committable.offsetsByTopicPartition().getOrDefault(topicPartition, null);
              return new TopicPartitionOffset(
                      topicPartition.topic(),
                      topicPartition.partition(),
                      offset == null ? null : offset.offset(),
                      offset == null ? null : offset.timestamp());
            })
            .collect(toList());

    List<TableTopicPartitionTransaction> tableTxIds = committable.getTableTxIds();

    Event commitReady = new Event(
            config.controlGroupId(),
            new TransactionDataComplete(commitId, assignments, tableTxIds));
    events.add(commitReady);

    Map<TopicPartition, Offset> offsets = committable.offsetsByTopicPartition();
    send(events, offsets, new ConsumerGroupMetadata(config.controlGroupId()));
    send(ImmutableList.of(), offsets, new ConsumerGroupMetadata(config.connectGroupId()));
  }

  @Override
  public void commit(CommittableSupplier committableSupplier) {
    throwExceptionIfCoordinatorIsTerminated();
    consumeAvailable(Duration.ZERO, envelope -> receive(envelope, committableSupplier));
  }

  @Override
  public void close() throws IOException {
    stop();
    maybeCoordinatorThread.ifPresent(CoordinatorThread::terminate);
  }

}