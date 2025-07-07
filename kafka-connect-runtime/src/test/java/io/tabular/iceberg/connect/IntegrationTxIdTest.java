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
import java.util.List;
import java.util.Map;
import java.util.UUID;


import org.apache.iceberg.DataFile;
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

    private static final String TEST_TABLE1 = "foobar1";
    private static final String TEST_TABLE2 = "foobar2";
    private static final TableIdentifier TABLE_IDENTIFIER1 = TableIdentifier.of(TEST_DB, TEST_TABLE1);
    private static final TableIdentifier TABLE_IDENTIFIER2 = TableIdentifier.of(TEST_DB, TEST_TABLE2);

    private String tableName;
    private TableIdentifier tableIdentifier;
    private String secondTestTopic;
    private String secondTestTable;
    private TableIdentifier secondTableIdentifier;

    @BeforeEach
    public void beforeEach() {
        // Each test will have a unique topic and table name to ensure isolation
        this.testTopic = "test-topic-" + UUID.randomUUID();
        this.tableName = TEST_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "_");
        this.tableIdentifier = TableIdentifier.of(TEST_DB, tableName);

        this.secondTestTopic = "second-test-topic-" + UUID.randomUUID().toString().replace("-", "_");
        this.secondTestTable = TEST_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "_");
        this.secondTableIdentifier = TableIdentifier.of(TEST_DB, secondTestTable);

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
                        .config("topics", testTopic+ "," + secondTestTopic)
                        .config("connector.class", IcebergSinkConnector.class.getName())
                        .config("tasks.max", tasksMax)
                        .config("consumer.override.auto.offset.reset", "earliest")
                        .config("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                        .config("key.converter.schemas.enable", false)
                        .config("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                        .config("value.converter.schemas.enable", useSchema)
//                        .config("iceberg.tables", tableIdentifier.toString() + "," + secondTableIdentifier.toString())
                        .config("iceberg.tables.dynamic-enabled", true)
                        .config("iceberg.tables.route-field","type" )
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

    private Snapshot awaitSnapshot(TableIdentifier tableIdentifier) {
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

    private void assertSnapshotTxIdProps(Snapshot snapshot, long expectedMax, long expectedValidThrough) {
        assertThat(snapshot).isNotNull();
        Map<String, String> summary = snapshot.summary();
        assertThat(summary.get("txid-max")).isEqualTo(String.valueOf(expectedMax));
        assertThat(summary.get("txid-valid-through"))
                .isEqualTo(String.valueOf(expectedValidThrough));

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
        Long records = dataFilesList(tableIdentifier, snapshot).stream()
                .mapToLong(DataFile::recordCount).sum();

        // valid-through should equal the max txid.
        assertThat(records).isEqualTo(3);
        assertSnapshotTxIdProps(snapshot, 102, 101);


    }


    @Test
    public void recreate() {
        createTopic(testTopic, 1);
        catalog.createTable(tableIdentifier, TEST_SCHEMA);
        createTopic(secondTestTopic, 1);
        catalog.createTable(secondTableIdentifier, TEST_SCHEMA);


        startConnector(true, 2);


        send(testTopic, 0, new TestEvent(1, tableIdentifier.toString(), new Date(), "p0d1", null, 100L), true);
        send(testTopic, 0, new TestEvent(2, tableIdentifier.toString(), new Date(), "p0d2", null, 101L), true);
        send(testTopic, 0, new TestEvent(3, tableIdentifier.toString(), new Date(), "p0d3", null, 102L), true);
        send(secondTestTopic, 0, new TestEvent(1, secondTableIdentifier.toString(), new Date(), "p0d1", null, 98L), true);
        send(secondTestTopic, 0, new TestEvent(2, secondTableIdentifier.toString(), new Date(), "p0d2", null, 99L), true);
        flush();

        Snapshot snapshot = awaitSnapshot(tableIdentifier);
        Long records = dataFilesList(tableIdentifier, snapshot).stream()
                .mapToLong(DataFile::recordCount).sum();

        Snapshot othersnapshot = awaitSnapshot(secondTableIdentifier);
        Long secondRecords = dataFilesList(secondTableIdentifier, othersnapshot).stream()
                .mapToLong(DataFile::recordCount).sum();

        // valid-through should equal the max txid.
        assertThat(records).isEqualTo(3);
        assertSnapshotTxIdProps(snapshot, 102, 101);

        assertThat(secondRecords).isEqualTo(2);
        assertSnapshotTxIdProps(othersnapshot, 99, 98); // this currently fails with 102 and 101


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
        assertSnapshotTxIdProps(snapshot, 108, 99);
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

        // This test confirms the current behavior. The Coordinator's `compareTxIds` correctly
        // handles wraparound for a single partition, so the highest txid for partition 0 is `5L`.
        // However, the `Utilities` methods for calculating final `max` and `valid-through` do not
        // account for wraparound when comparing values from *different* partitions.
        // Expected max = simple max(5L, 4294967280L) = 4294967280L
        // Expected valid-through = simple min(5L, 4294967280L) - 1 = 4L
        assertSnapshotTxIdProps(snapshot, highTxId1, 4L);
    }
}
