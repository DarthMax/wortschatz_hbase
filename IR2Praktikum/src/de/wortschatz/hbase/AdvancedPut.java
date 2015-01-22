package de.wortschatz.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Marcel Kisilowski on 22.01.15.
 */
public class AdvancedPut extends Put {
    public AdvancedPut(byte[] row) {
        super(row);
    }
    public Put add(String columnFamily, String qualifier,String value) {
        this.add(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier),Bytes.toBytes(value));
        return this;
    }
}
