package de.wortschatz.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;

/**
 * Created by max on 24.01.15.
 */
public class SentenceEmigrationManager {
    public HBaseCRUDer hBaseCRUDer;
    public SqlDataGetter sqlDataGetter;

    public final String tableName = "sentences";

    public SentenceEmigrationManager() {
        this.hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        hBaseCRUDer.setTable(tableName);

        this.sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
    }

    public void migrate(){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());
        ArrayList<Sentence> sentences;

        int offset = 0;
        int limit = 10000;

        do {
            sentences = dataGetter.getSentences(offset, limit);

            if (!sentences.isEmpty()) {
                ArrayList<Put> putlist = new ArrayList<>();
                for (Sentence sentence : sentences) {
                    byte[] key = Bytes.toBytes(sentence.getId());

                    Put put = new Put(key);

                    put.add(Bytes.toBytes("data"),Bytes.toBytes("text"),Bytes.toBytes(sentence.getText()));
                    for(String word : sentence.getWords()) {
                        put.add(Bytes.toBytes("words"), Bytes.toBytes(word), Bytes.toBytes(word));
                    }
                    for(String source : sentence.getSources()) {
                        put.add(Bytes.toBytes("sources"), Bytes.toBytes(source), Bytes.toBytes(source));
                    }

                    putlist.add(put);
                }
                hBaseCRUDer.updateTable(putlist);
            }
            offset += limit;
        } while(!sentences.isEmpty());

    }

    public static void main(String[] args) {
        new SentenceEmigrationManager().migrate();
    }
}
