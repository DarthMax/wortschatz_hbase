package de.wortschatz.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

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

    public void updateTable(Put put) {
        try {
            table.put(put);
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }
    }
    public void updateTable(List<Put> put) {
        try {
            table.put(put);
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }
    }

    public void setTable(String tableName) {
        try {
            if(table != null && !table.getTableDescriptor().getNameAsString().equals(tableName)) {
                table = new HTable(conf,tableName);
            }
            table = new HTable(conf,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Scan getScan(String startRow,String stopRow, long maxResultSet){
        Scan scan = new Scan(Bytes.toBytes(startRow),Bytes.toBytes(stopRow));
        scan.setMaxResultSize(maxResultSet);
        return scan;
    }

    public Result get(Get get) {
        try {
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }





    /**
     *
     * @param key
     * @return
     */
    public Scan getScan(String key){
        Scan scan = new Scan(Bytes.toBytes(key+'|'),Bytes.toBytes(key+"|z"));
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
