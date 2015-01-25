package de.wortschatz.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Array;
import java.sql.SQLData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Marcel Kisilowski on 22.01.15.
 */
public class CooccurrenceEmigrationManager {
    public HBaseCRUDer hBaseCRUDer;
    public SqlDataGetter sqlDataGetter;

    public final String tableName = "cooccurrences1M";
    private final String columnFamily = "data";


    public CooccurrenceEmigrationManager() {
        this.hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        hBaseCRUDer.setTable(tableName);

        this.sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
    }

    public void migrate() {
        String[] cooccurrence_types = {"co_s","co_n"};

        for (String type:cooccurrence_types) {
            migrateCooccurrences(type);
        }
    }


    private void migrateCooccurrences(String type){
        CooccurrenceKeyGenerator generator = new CooccurrenceKeyGenerator(type);
        ArrayList<Cooccurrence> cos;
        int offset = 0;
        int limit = 10000;

        do {
            cos = this.sqlDataGetter.getCooccurrenceData(type,offset,limit);

            if (!cos.isEmpty()) {
                ArrayList<Put> putlist = new ArrayList<>();
                for (Cooccurrence co : cos) {
                    String key = generator.keyFor(co);
                    Put put = new Put(Bytes.toBytes(key));
                    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("sig"), Bytes.toBytes(co.getSignificance()));
                    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("freq"), Bytes.toBytes(co.getFrequency()));
                    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("word1"), Bytes.toBytes(co.getWord1()));
                    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("word2"), Bytes.toBytes(co.getWord2()));
                    putlist.add(put);
                }
                hBaseCRUDer.updateTable(putlist);
            }
            offset += limit;
        } while(!cos.isEmpty());

    }

    public static void main(String[] args) {
        new CooccurrenceEmigrationManager().migrate();
    }
}
