package de.wortschatz.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;

/**
 * Created by max on 24.01.15.
 */
public class SourceEmigrationManager extends EmigrationManager{


    public SourceEmigrationManager() {
        this.tableName = "sources1M";
        this.columnFamilies = new String[]{"sentence_ids"};
        hBaseCRUDer.setTable(this.tableName);
    }

    public void migrate(){
        super.migrate();
        migrateSentenceIds();
    }

    private void migrateSentenceIds(){
        String query = "select " +
                "sources.source as source, " +
                "inv_so.s_id as s_id " +
                "from sources, inv_so " +
                "where sources.so_id=inv_so.so_id";

        migrateTuple(query, "sentence_ids", "", "long");
    }

    public static void main(String[] args) {
        new SourceEmigrationManager().migrate();
    }
}
