package io.tabular.iceberg.connect.channel;

import org.apache.avro.Schema;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;

public class TxIdValidUntilEvent extends Event {

    private final int partition;
    private final long txId;

    public TxIdValidUntilEvent(Schema schema, int partition, long txId) {
        super(schema);
        this.partition = partition;
        this.txId = txId;
    }

    public TxIdValidUntilEvent(String groupId, Payload payload, int partition, long txId) {
        super(groupId, payload);
        this.partition = partition;
        this.txId = txId;
    }

    public int getPartition() {
        return partition;
    }

    public long getTxId() {
        return txId;
    }
}