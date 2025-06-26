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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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

  public CommitterImpl(
          SinkTaskContext context, IcebergSinkConfig config, Catalog catalog, Worker worker) {
    this(context, config, catalog, new KafkaClientFactory(config.kafkaProps()), worker);
  }

  private CommitterImpl(
          SinkTaskContext context,
          IcebergSinkConfig config,
          Catalog catalog,
          KafkaClientFactory kafkaClientFactory,
          Worker worker) {
    this(
            context,
            config,
            kafkaClientFactory,
            new CoordinatorThreadFactoryImpl(catalog, kafkaClientFactory),
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

  private void sendCommitResponse(UUID commitId, CommittableSupplier committableSupplier) {
    Committable committable = committableSupplier.committable(commitId);
    List<Event> events = Lists.newArrayList();

    committable.writerResults().stream()
            .filter(java.util.Objects::nonNull)
            .forEach(
                    writerResult -> {
                      Event commitResponse =
                              new Event(
                                      config.controlGroupId(),
                                      new DataWritten(
                                              writerResult.partitionStruct(),
                                              commitId,
                                              TableReference.of(config.catalogName(), writerResult.tableIdentifier()),
                                              writerResult.dataFiles(),
                                              writerResult.deleteFiles()));
                      events.add(commitResponse);
                    });

    List<TopicPartitionOffset> assignments =
            context.assignment().stream()
                    .map(
                            topicPartition -> {
                              Offset offset =
                                      committable.offsetsByTopicPartition() == null
                                              ? null
                                              : committable.offsetsByTopicPartition().getOrDefault(topicPartition, null);
                              return new TopicPartitionOffset(
                                      topicPartition.topic(),
                                      topicPartition.partition(),
                                      offset == null ? null : offset.offset(),
                                      offset == null ? null : offset.timestamp());
                            })
                    .collect(toList());

    List<TableTopicPartitionTransaction> tableTxIds = committable.getTableTxIds();
    Event commitReady =
            new Event(
                    config.controlGroupId(), new TransactionDataComplete(commitId, assignments, tableTxIds));
    events.add(commitReady);

    Map<TopicPartition, Offset> offsets = committable.offsetsByTopicPartition();
    if (offsets != null && !offsets.isEmpty()) {
      send(events, offsets, new ConsumerGroupMetadata(config.controlGroupId()));
      send(ImmutableList.of(), offsets, new ConsumerGroupMetadata(config.connectGroupId()));
    } else {
      // Send all events in a single transaction, even if there are no offsets.
      send(events, ImmutableMap.of(), null);
    }
  }

  public void process() {
    throwExceptionIfCoordinatorIsTerminated();
    LOG.info("TRACE: I am being called process() in CommitterImpl");
    // Non-blocking poll to look for new events. This should be called periodically.
    consumeAvailable(
            Duration.ZERO,
            envelope -> {
              if (envelope.event().type() == PayloadType.START_COMMIT) {
                UUID commitId = ((StartCommit) envelope.event().payload()).commitId();
                worker.setCurrentCommitId(commitId);
              }
              return true;
            });
  }

  // @Override
  // public boolean commit(CommittableSupplier committableSupplier) {
  //   throwExceptionIfCoordinatorIsTerminated();

  //   final AtomicBoolean responseSent = new AtomicBoolean(false);
  //   // Check for a pending START_COMMIT event or an active commit ID.
  //   consumeAvailable(
  //           Duration.ZERO,
  //           envelope -> {
  //             if (envelope.event().type() == PayloadType.START_COMMIT) {
  //               UUID commitId = ((StartCommit) envelope.event().payload()).commitId();
  //               // We have the ID and the supplier, so we can send the response directly.
  //               worker.setCurrentCommitId(commitId);
  //               sendCommitResponse(commitId, committableSupplier);
  //               responseSent.set(true);
  //             }
  //             return true;
  //           });
  //   // If no START_COMMIT event was found, check if we have an active commit ID.
  //   if (!responseSent.get()) {
  //     UUID commitId = worker.currentCommitId();
  //     if (commitId != null) {
  //       sendCommitResponse(commitId, committableSupplier);
  //       responseSent.set(true);
  //     }
  //   }

  //   if (!responseSent.get()) {
  //     LOG.warn(
  //             "Commit called but no pending START_COMMIT event or active commit ID found, skipping response.");
  //   }

  //   return responseSent.get();
  // }
  @Override
  public boolean commit(CommittableSupplier committableSupplier) {
    throwExceptionIfCoordinatorIsTerminated();
    LOG.info("TRACE: I am being called commit() in CommitterImpl");
    UUID commitId = worker.currentCommitId();
    if (commitId != null) {
      sendCommitResponse(commitId, committableSupplier);
      return true;
    } else {
      LOG.warn("Commit called but no active commit ID found, skipping response.");
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    stop();
    maybeCoordinatorThread.ifPresent(CoordinatorThread::terminate);
  }
}
