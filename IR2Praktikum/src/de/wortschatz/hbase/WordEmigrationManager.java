package de.wortschatz.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by max on 24.01.15.
 */
public class WordEmigrationManager {
    public HBaseCRUDer hBaseCRUDer;
    public SqlDataGetter sqlDataGetter;

    public final String tableName = "words1M";

    public WordEmigrationManager() {
        this.hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        hBaseCRUDer.setTable(tableName);

        this.sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
    }
    private void createTable() {
        HTableDescriptor tableDescriptor = new
                HTableDescriptor(TableName.valueOf(tableName));
        tableDescriptor.addFamily(new HColumnDescriptor("data"));
        tableDescriptor.addFamily(new HColumnDescriptor("sentence_ids"));
        try {
           HBaseAdmin admin = new HBaseAdmin(HBaseConnector.get_connection());
            admin.createTable(tableDescriptor);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void migrate(){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());
        ArrayList<Word> words;
        int offset = 0;
        int limit = 1;
        createTable();
        do {
            words = dataGetter.getWords(offset,limit);
            System.out.println("limit = " + limit);
            System.out.println("offset = " + offset);
            if (!words.isEmpty()) {
                ArrayList<Put> putlist = new ArrayList<>();
                for (Word word : words) {
                    byte[] key = Bytes.toBytes(word.getValue());
                    Put put = new Put(key);
                    put.add(Bytes.toBytes("data"),Bytes.toBytes("freq"),Bytes.toBytes(word.getFrequency()));
                    for(long sentence_id : word.getSentenceIDs()) {
                        put.add(Bytes.toBytes("sentence_ids"), Bytes.toBytes(sentence_id), Bytes.toBytes(sentence_id));
                    }
                    putlist.add(put);
                    System.out.println("Word:" + word);
                }
                hBaseCRUDer.updateTable(putlist);
            }
            offset += limit;
        } while(!words.isEmpty());

    }

    public static void main(String[] args) {
        new WordEmigrationManager().migrate();
    }
}
