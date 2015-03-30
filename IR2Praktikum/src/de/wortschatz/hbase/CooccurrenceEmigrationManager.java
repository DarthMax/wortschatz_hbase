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
 * Emigration manager that migrates cooccurrence data from SQL to HBase.
 * The row key is contructet by the pattern word1|type|inv_sig|word2.
 * The both words, significance and frequency are stored in the column family 'data'
 */
public class CooccurrenceEmigrationManager extends EmigrationManager{

    public CooccurrenceEmigrationManager(){
        this.tableName = "cooccurrences";
        this.columnFamilies = new String[]{"data"};
        hBaseCRUDer.setTable(this.tableName);
    }

    /**
     * Migrate sentence and neighbourhood cooccurrences
     */
    public void migrate() {
        super.migrate();
        String[] cooccurrence_types = {"co_n"};

        for (String type:cooccurrence_types) {
            migrateCooccurrences(type);
        }
    }


    /**
     * Migrate all cooccurrences of the given type. Data is collected in chunks of 10000 rows
     * @param type The cooccurrence type to migrate specified by the table name of cooccurrence data
     */
    private void migrateCooccurrences(String type){
        CooccurrenceKeyGenerator generator = new CooccurrenceKeyGenerator(type);
        ArrayList<Cooccurrence> cos;
        int offset = 0;
        int limit = 10;

        do {
            cos = this.sqlDataGetter.getCooccurrenceData(type,offset,limit);

            if (!cos.isEmpty()) {
                ArrayList<Put> putlist = new ArrayList<>();
                for (Cooccurrence co : cos) {
                    String key = generator.keyFor(co);
                    Put put = new Put(Bytes.toBytes(key));
                    put.add(Bytes.toBytes(columnFamilies[0]), Bytes.toBytes("sig"), Bytes.toBytes(co.getSignificance()));
                    put.add(Bytes.toBytes(columnFamilies[0]), Bytes.toBytes("freq"), Bytes.toBytes(co.getFrequency()));
                    put.add(Bytes.toBytes(columnFamilies[0]), Bytes.toBytes("word1"), Bytes.toBytes(co.getWord1()));
                    put.add(Bytes.toBytes(columnFamilies[0]), Bytes.toBytes("word2"), Bytes.toBytes(co.getWord2()));
                    putlist.add(put);
                }
                hBaseCRUDer.updateTable(putlist);
            }
            if(limit < 1000000) {limit = limit * 100;}
            offset += limit;

        } while(!cos.isEmpty());

    }

    public static void main(String[] args) {
        new CooccurrenceEmigrationManager().migrate();
    }
}
