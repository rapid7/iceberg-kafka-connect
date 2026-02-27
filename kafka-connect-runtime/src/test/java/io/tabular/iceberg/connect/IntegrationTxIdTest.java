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

import static io.tabular.iceberg.connect.TestEvent.TEST_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.UUID;


import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegrationTxIdTest extends IntegrationTestBase {

    private static final String TEST_DB = "test";
    private static final String TEST_TABLE_PREFIX = "foobar_";
    private static final int TEST_TOPIC_PARTITIONS = 3;

    private String tableName;
    private TableIdentifier tableIdentifier;

    @BeforeEach
    public void beforeEach() {
        // Each test will have a unique topic and table name to ensure isolation
        this.testTopic = "test-topic-" + UUID.randomUUID();
        this.tableName = TEST_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "_");
        this.tableIdentifier = TableIdentifier.of(TEST_DB, tableName);

        // Namespace can be shared, so create it if it doesn't exist
        if (!((SupportsNamespaces) catalog).namespaceExists(Namespace.of(TEST_DB))) {
            ((SupportsNamespaces) catalog).createNamespace(Namespace.of(TEST_DB));
        }
    }

    @AfterEach
    public void afterEach() {
        context.stopConnector(connectorName);
        deleteTopic(testTopic);
        catalog.dropTable(tableIdentifier);
    }

    private void startConnector(boolean useSchema, int tasksMax) {
        KafkaConnectContainer.Config connectorConfig =
                new KafkaConnectContainer.Config(connectorName)
                        .config("topics", testTopic)
                        .config("connector.class", IcebergSinkConnector.class.getName())
                        .config("tasks.max", tasksMax)
                        .config("consumer.override.auto.offset.reset", "earliest")
                        .config("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                        .config("key.converter.schemas.enable", false)
                        .config("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                        .config("value.converter.schemas.enable", useSchema)
                        .config("iceberg.tables", tableIdentifier.toString())
                        .config("iceberg.control.commit.interval-ms", 5000) // Longer commit interval
                        .config("iceberg.control.commit.timeout-ms", Integer.MAX_VALUE)
                        .config("iceberg.kafka.auto.offset.reset", "earliest");

        context.connectorCatalogProperties().forEach(connectorConfig::config);
        context.startConnector(connectorConfig);
    }

    private Snapshot awaitSnapshot() {
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(
                        () -> {
                            Table table = catalog.loadTable(tableIdentifier);
                            assertThat(table.snapshots()).hasSizeGreaterThanOrEqualTo(1);
                        });
        return catalog.loadTable(tableIdentifier).currentSnapshot();
    }

    private void assertSnapshotTxIdProps(Snapshot snapshot, long expectedMax, long expectedValidThrough, String commitType) {
        assertThat(snapshot).isNotNull();
        Map<String, String> summary = snapshot.summary();
        assertThat(summary.get("txid-max")).isEqualTo(String.valueOf(expectedMax));
        assertThat(summary.get("txid-valid-through"))
                .isEqualTo(String.valueOf(expectedValidThrough));
        assertThat(summary.get("commit.type")).isEqualTo(String.valueOf(commitType));
    }

    @Test
    public void testIcebergSinkTxIdTracking() {
        createTopic(testTopic, 1);
        catalog.createTable(tableIdentifier, TEST_SCHEMA);
        startConnector(true, 1);

        send(testTopic, 0, new TestEvent(1, "type1", new Date(), "p0d1", null, 100L), true);
        send(testTopic, 0, new TestEvent(2, "type1", new Date(), "p0d2", null, 101L), true);
        send(testTopic, 0, new TestEvent(3, "type1", new Date(), "p0d3", null, 102L), true);
        flush();

        Snapshot snapshot = awaitSnapshot();

        // valid-through should equal the max txid.
        assertSnapshotTxIdProps(snapshot, 102, 102, "full");
    }

    @Test
    public void testIcebergSinkMixedTxIdPartitions() {
        createTopic(testTopic, TEST_TOPIC_PARTITIONS);
        catalog.createTable(tableIdentifier, TEST_SCHEMA);
        startConnector(true, 3);

        send(testTopic, 0, new TestEvent(1, "type1", new Date(), "p0d1", null, 100L), true);
        send(testTopic, 1, new TestEvent(2, "type1", new Date(), "p1d1", null, 101L), true);
        send(testTopic, 2, new TestEvent(3, "type1", new Date(), "p2d1", null, 108L), true);
        flush();

        Snapshot snapshot = awaitSnapshot();
        // The max txid is the highest across all partitions, and valid-through is one less than that.
        assertSnapshotTxIdProps(snapshot, 108, 99, "full");
    }

    @Test
    public void testIcebergSinkTxIdWraparound() {
        createTopic(testTopic, 3);
        catalog.createTable(tableIdentifier, TEST_SCHEMA);
        startConnector(true, 1);

        long highTxId1 = 4_294_967_290L;
        long lowTxIdAfterWrap = 5L;
        long highTxId2 = 4_294_967_280L;

        send(testTopic, 0, new TestEvent(1, "type1", new Date(), "p0d1", null, highTxId1), true);
        send(testTopic, 1, new TestEvent(2, "type1", new Date(), "p1d2", null, lowTxIdAfterWrap), true);
        send(testTopic, 2, new TestEvent(3, "type1", new Date(), "p2d1", null, highTxId2), true);
        flush();

        Snapshot snapshot = awaitSnapshot();

        // Wraparound:
        // - Partition 0: highTxId1 (4_294_967_290)
        // - Partition 1: lowTxIdAfterWrap (5)
        // - Partition 2: highTxId2 (4_294_967_280)
        // The max should be the lowest value (after wraparound): 5
        // The valid-through should be min of before-wrap values minus 1: 4_294_967_280 - 1
        assertSnapshotTxIdProps(snapshot, lowTxIdAfterWrap, highTxId2 - 1, "full");
    }

    @Test
    public void testIcebergSinkTxIdAllJustBeforeWraparound() {
        createTopic(testTopic, 3);
        catalog.createTable(tableIdentifier, TEST_SCHEMA);
        startConnector(true, 1);

        long txId1 = 4_294_967_294L;
        long txId2 = 4_294_967_295L;
        long txId3 = 4_294_967_296L;

        send(testTopic, 0, new TestEvent(1, "type1", new Date(), "p0d1", null, txId1), true);
        send(testTopic, 1, new TestEvent(2, "type1", new Date(), "p1d1", null, txId2), true);
        send(testTopic, 2, new TestEvent(3, "type1", new Date(), "p2d1", null, txId3), true);
        flush();

        Snapshot snapshot = awaitSnapshot();

        // All values just before wraparound, no wraparound detected
        // max = 4_294_967_296, valid-through = min - 1 = 4_294_967_293
        assertSnapshotTxIdProps(snapshot, txId3, txId1 - 1, "full");
    }

    @Test
    public void testIcebergSinkTxIdAllJustAfterWraparound() {
        createTopic(testTopic, 3);
        catalog.createTable(tableIdentifier, TEST_SCHEMA);
        startConnector(true, 1);

        long txId1 = 1L;
        long txId2 = 2L;
        long txId3 = 3L;

        send(testTopic, 0, new TestEvent(1, "type1", new Date(), "p0d1", null, txId1), true);
        send(testTopic, 1, new TestEvent(2, "type1", new Date(), "p1d1", null, txId2), true);
        send(testTopic, 2, new TestEvent(3, "type1", new Date(), "p2d1", null, txId3), true);
        flush();

        Snapshot snapshot = awaitSnapshot();

        // All values just after wraparound, no wraparound detected
        // max = 3, valid-through = min - 1 = 0
        assertSnapshotTxIdProps(snapshot, txId3, 0, "full");
    }

    @Test
    public void testIcebergSinkTxIdWraparoundBoundary() {
        createTopic(testTopic, 4);
        catalog.createTable(tableIdentifier, TEST_SCHEMA);
        startConnector(true, 1);

        long txId1 = 4_294_967_295L;
        long txId2 = 4_294_967_296L;
        long txId3 = 1L;
        long txId4 = 2L;

        send(testTopic, 0, new TestEvent(1, "type1", new Date(), "p0d1", null, txId1), true);
        send(testTopic, 1, new TestEvent(2, "type1", new Date(), "p1d1", null, txId2), true);
        send(testTopic, 2, new TestEvent(3, "type1", new Date(), "p2d1", null, txId3), true);
        send(testTopic, 3, new TestEvent(4, "type1", new Date(), "p3d1", null, txId4), true);
        flush();

        Snapshot snapshot = awaitSnapshot();

        // Wraparound at the boundary: partitions span across the wrap point
        // max = 2 (highest after wrap), valid-through = min of before-wrap - 1 = 4_294_967_294
        assertSnapshotTxIdProps(snapshot, txId4, txId1 - 1, "full");
    }
}
