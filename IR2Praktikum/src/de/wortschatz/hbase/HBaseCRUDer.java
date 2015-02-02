package de.wortschatz.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Marcel Kisilowski on 22.01.15.
 */
public class HBaseCRUDer {
    private HTable table;

    private Configuration conf;

    /**
     *
     * @param conf
     */
    public HBaseCRUDer(Configuration conf){
        this.conf=conf;
        table = null;
    }

    /**
     * Perform a Put
     * @param put
     */
    public void updateTable(Put put) {
        try {
            table.put(put);
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }
    }

    /**
     *  Perform a Put with a list look: {@link #updateTable(Put put) updateTable}
     * @param put
     */
    public void updateTable(List<Put> put) {
        try {
            table.put(put);
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }
    }

    /**
     * Set current table
     * @param tableName
     */
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

    /**
     * Prepare a Scan-Object for later use.
     * @param startRow
     * @param stopRow
     * @param maxResultSet
     * @return
     */
    public Scan getScan(String startRow,String stopRow, long maxResultSet){
        Scan scan = new Scan(Bytes.toBytes(startRow),Bytes.toBytes(stopRow));
        scan.setMaxResultSize(maxResultSet);
        return scan;
    }

    /**
     * Perform a get request.
     * @param get
     * @return
     */
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

    /**
     * Perform a Scan request.
     * @param scan
     * @return
     */
    public ResultScanner scanTable(Scan scan) {
        ResultScanner result = null;
        try {
            result = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Convert Result to Cooccurrences-Resultlist
     * todo -> very ungeneric, maybe wrong class
     * @param scanner
     * @return
     */
    public ArrayList<Cooccurrence> convertToCooccurrences(ResultScanner scanner) {
        Result row;
        ArrayList<Cooccurrence> resultList = new ArrayList<>();
        try {
            while ((row = scanner.next())!=null) {
                resultList.add(new Cooccurrence(row));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultList;
    }

}
