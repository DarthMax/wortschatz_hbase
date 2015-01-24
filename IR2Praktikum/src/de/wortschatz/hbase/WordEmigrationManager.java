package de.wortschatz.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;

/**
 * Created by max on 24.01.15.
 */
public class WordEmigrationManager {
    public HBaseCRUDer hBaseCRUDer;
    public SqlDataGetter sqlDataGetter;

    public final String tableName = "words";

    public WordEmigrationManager() {
        this.hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        hBaseCRUDer.setTable(tableName);

        this.sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
    }

    public void migrate(){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());
        ArrayList<Word> words;

        int offset = 0;
        int limit = 10000;

        do {
            words = dataGetter.getWords(offset,limit);

            if (!words.isEmpty()) {
                ArrayList<Put> putlist = new ArrayList<>();
                for (Word word : words) {
                    byte[] key = Bytes.toBytes(word.getValue());

                    Put put = new Put(key);

                    put.add(Bytes.toBytes("data"),Bytes.toBytes("freq"),Bytes.toBytes(word.getFrequency()));
                    for(int sentence_id : word.getSentenceIDs()) {
                        put.add(Bytes.toBytes("sentence_ids"), Bytes.toBytes(sentence_id), Bytes.toBytes(sentence_id));
                    }
                    putlist.add(put);
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
