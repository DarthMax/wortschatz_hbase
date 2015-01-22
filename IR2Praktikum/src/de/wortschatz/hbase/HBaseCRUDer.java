package de.wortschatz.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Marcel Kisilowski on 22.01.15.
 */
public class HBaseCRUDer {
    private HTable table;
    private Configuration conf;
    public HBaseCRUDer(Configuration conf){
        this.conf=conf;
    }

    public Put createPut(String tableName,String rowName,String columnFamily, String qualifier,String value){
        AdvancedPut put = null;
        boolean ret = false;
        try {
            if(table != null && !table.getTableDescriptor().getNameAsString().equals(tableName)) {
                table = new HTable(conf,tableName);
            }
            put = new AdvancedPut(Bytes.toBytes(rowName));
            put.add(columnFamily,qualifier,value);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return put;
    }

    public void read(){

    }

}
