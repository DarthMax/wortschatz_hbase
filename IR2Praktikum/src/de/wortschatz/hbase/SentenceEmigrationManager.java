package de.wortschatz.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.compiler.ir.Tuple;

import java.util.ArrayList;

/**
 * Created by max on 24.01.15.
 */
public class SentenceEmigrationManager extends EmigrationManager {
    public SentenceEmigrationManager(){
        this.tableName = "sentences";
        this.columnFamilies = new String[]{"data", "words", "sources"};
        hBaseCRUDer.setTable(this.tableName);
    }

    public void migrate() {
        //super.migrate();
        migrateSentences();
        migrateWords();
        migrateSources();
    }

    private void migrateSentences() {
        String query = "select " +
                "CAST(s_id as char), " +
                "sentence " +
                "from sentences";
        migrateTuple(query, "data", "value", "string");
    }

    private void migrateWords(){
        String query = "select " +
                "CAST(inv_w.s_id as char) as s_id, " +
                "w.word as word " +
                "from words w, inv_w " +
                "where w.w_id=inv_w.w_id";
        migrateTuple(query, "words", "", "string");
    }

    private void migrateSources(){
        String query = "select " +
                "CAST(inv_so.s_id as char) as s_id, " +
                "sources.source as source " +
                "from sources, inv_so " +
                "where sources.so_id=inv_so.so_id";
        migrateTuple(query, "sources", "", "string");
    }


    public static void main(String[] args) {
        new SentenceEmigrationManager().migrate();
    }
}
