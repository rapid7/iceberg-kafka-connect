package io.tabular.iceberg.connect.channel;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class AvroSchemaUtil {
    public static Schema createDataWrittenEventSchema() {
        return SchemaBuilder.record("DataWrittenEvent")
                .namespace("io.tabular.iceberg.connect.channel")
                .fields()
                .name("partition").type().intType().noDefault()
                .name("txid").type().longType().noDefault()
                .endRecord();
    }
}