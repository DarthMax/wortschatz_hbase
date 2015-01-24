package de.wortschatz.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;

/**
 * Created by max on 24.01.15.
 */
public class SourceEmigrationManager {
    public HBaseCRUDer hBaseCRUDer;
    public SqlDataGetter sqlDataGetter;

    public final String tableName = "sources";

    public SourceEmigrationManager() {
        this.hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        hBaseCRUDer.setTable(tableName);

        this.sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
    }

    public void migrate(){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());
        ArrayList<Source> sources;

        int offset = 0;
        int limit = 10000;

        do {
            sources = dataGetter.getSources(offset,limit);

            if (!sources.isEmpty()) {
                ArrayList<Put> putlist = new ArrayList<>();
                for (Source source : sources) {
                    byte[] key = Bytes.toBytes(source.getValue());

                    Put put = new Put(key);

                    for(int sentence : source.getSentences()) {
                        put.add(Bytes.toBytes("sentence_ids"), Bytes.toBytes(sentence), Bytes.toBytes(sentence));
                    }
                    putlist.add(put);
                }
                hBaseCRUDer.updateTable(putlist);
            }
            offset += limit;
        } while(!sources.isEmpty());

    }

    public static void main(String[] args) {
        new SourceEmigrationManager().migrate();
    }
}
