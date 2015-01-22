package de.wortschatz.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Created by Marcel Kisilowski on 22.01.15.
 */
public class HBaseCRUDer {
    private HTable table;
    private Configuration conf;
    public HBaseCRUDer(Configuration conf){
        this.conf=conf;
        table = null;
    }

    public Put createPut(String rowName,String columnFamily, String qualifier,String value){
        AdvancedPut put = null;
        boolean ret = false;
        put = new AdvancedPut(Bytes.toBytes(rowName));
        put.add(columnFamily,qualifier,value);
        return put;
    }

    public void updateTable(Put put) {
        try {
            table.put(put);
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }
    }

    public void setTable(String tableName) throws IOException {
        if(table != null && !table.getTableDescriptor().getNameAsString().equals(tableName)) {
            table = new HTable(conf,tableName);
        }

    }

    public Scan getScan(String startRow,String stopRow, long maxResultSet){
        Scan scan = new Scan(Bytes.toBytes(startRow),Bytes.toBytes(stopRow));
        scan.setMaxResultSize(maxResultSet);
        return scan;
    }
    public ResultScanner scanTable(Scan scan) {
        ResultScanner result = null;
        try {
            result = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
