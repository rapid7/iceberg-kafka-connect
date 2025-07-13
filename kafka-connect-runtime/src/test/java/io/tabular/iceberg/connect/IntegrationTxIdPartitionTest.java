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

public class IntegrationTxIdPartitionTest extends IntegrationTestBase {

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

    private void startConnector(boolean useSchema, int tasksMax, String topics) {
        KafkaConnectContainer.Config connectorConfig =
                new KafkaConnectContainer.Config(connectorName)
                        .config("topics", topics)
                        .config("connector.class", IcebergSinkConnector.class.getName())
                        .config("tasks.max", tasksMax)
                        .config("consumer.override.auto.offset.reset", "earliest")
                        .config("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                        .config("key.converter.schemas.enable", false)
                        .config("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                        .config("value.converter.schemas.enable", useSchema)
                        .config("iceberg.tables.dynamic-enabled", true)
                        .config("iceberg.tables.route-field", "type")
                        .config("iceberg.control.commit.interval-ms", 3000) // Longer commit interval
                        .config("iceberg.control.commit.timeout-ms", Integer.MAX_VALUE)
                        .config("iceberg.kafka.auto.offset.reset", "earliest");

        context.connectorCatalogProperties().forEach(connectorConfig::config);
        context.startConnector(connectorConfig);
    }


    private Snapshot awaitSnapshot(TableIdentifier tableIdentifiered) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(
                        () -> {
                            Table table = catalog.loadTable(tableIdentifiered);
                            assertThat(table.snapshots()).hasSizeGreaterThanOrEqualTo(1);
                        });
        return catalog.loadTable(tableIdentifiered).currentSnapshot();
    }

    private void assertSnapshotTxIdProps(Snapshot snapshot, long expectedMax, long expectedValidThrough) {
        assertThat(snapshot).isNotNull();
        Map<String, String> summary = snapshot.summary();
        assertThat(summary.get("txid-max")).isEqualTo(String.valueOf(expectedMax));
        assertThat(summary.get("txid-valid-through"))
                .isEqualTo(String.valueOf(expectedValidThrough));

    }

    private void assertRecordCount(Snapshot snapshot, int recordCount) {
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.summary().get("total-records")).isEqualTo(String.valueOf(recordCount));
    }

    @Test
    public void MultiTopicConnectorCorrectlySetsSnapshotMetadata() {
        createTopic(testTopic, 1);
        catalog.createTable(tableIdentifier, TEST_SCHEMA);
        createTopic(secondTestTopic, 1);
        catalog.createTable(secondTableIdentifier, TEST_SCHEMA);


        startConnector(true, 2, testTopic+ "," + secondTestTopic);


        send(testTopic, 0, new TestEvent(1, tableIdentifier.toString(), new Date(), "p0d1", null, 100L), true);
        send(testTopic, 0, new TestEvent(2, tableIdentifier.toString(), new Date(), "p0d2", null, 101L), true);
        send(testTopic, 0, new TestEvent(3, tableIdentifier.toString(), new Date(), "p0d3", null, 102L), true);
        send(secondTestTopic, 0, new TestEvent(1, secondTableIdentifier.toString(), new Date(), "p0d1", null, 98L), true);
        send(secondTestTopic, 0, new TestEvent(2, secondTableIdentifier.toString(), new Date(), "p0d2", null, 99L), true);
        flush();

        Snapshot snapshotFirstTable = awaitSnapshot(tableIdentifier);

        Snapshot snapshotSecondTable = awaitSnapshot(secondTableIdentifier);



        assertRecordCount(snapshotFirstTable, 3);
        assertSnapshotTxIdProps(snapshotFirstTable, 102, 102);

        assertRecordCount(snapshotSecondTable, 2);
        assertSnapshotTxIdProps(snapshotSecondTable, 99, 99); // this currently fails with 102 and 101

    }



    @Test
    public void MultiTopicMultiPartitionConnectorCorrectlySetsSnapshotMetadata() {
        createTopic(testTopic, 2);
        catalog.createTable(tableIdentifier, TEST_SCHEMA);
        createTopic(secondTestTopic, 2);
        catalog.createTable(secondTableIdentifier, TEST_SCHEMA);


        startConnector(true, 4, testTopic+ "," + secondTestTopic);


        send(testTopic, 0, new TestEvent(1, tableIdentifier.toString(), new Date(), "p0d1", null, 100L), true);
        send(testTopic, 0, new TestEvent(2, tableIdentifier.toString(), new Date(), "p0d2", null, 101L), true);
        send(testTopic, 0, new TestEvent(3, tableIdentifier.toString(), new Date(), "p0d3", null, 102L), true);
        send(testTopic, 1, new TestEvent(4, tableIdentifier.toString(), new Date(), "p0d4", null, 103L), true);
        send(testTopic, 1, new TestEvent(5, tableIdentifier.toString(), new Date(), "p0d5", null, 104L), true);
        send(secondTestTopic, 0, new TestEvent(1, secondTableIdentifier.toString(), new Date(), "p0d1", null, 97L), true);
        send(secondTestTopic, 0, new TestEvent(2, secondTableIdentifier.toString(), new Date(), "p0d2", null, 98L), true);
        send(secondTestTopic, 1, new TestEvent(3, secondTableIdentifier.toString(), new Date(), "p0d3", null, 99L), true);
        flush();

        Snapshot snapshotFirstTable = awaitSnapshot(tableIdentifier);

        Snapshot snapshotSecondTable = awaitSnapshot(secondTableIdentifier);



        assertRecordCount(snapshotFirstTable, 5);
        assertSnapshotTxIdProps(snapshotFirstTable, 104, 101);

        assertRecordCount(snapshotSecondTable, 3);
        assertSnapshotTxIdProps(snapshotSecondTable, 99, 97); // this currently fails with 102 and 101

    }


}
