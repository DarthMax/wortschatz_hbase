package de.wortschatz.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.compiler.ir.Tuple;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by max on 24.01.15.
 */
public class WordEmigrationManager extends EmigrationManager{

    public WordEmigrationManager(){
        this.tableName = "words";
        this.columnFamilies = new String[]{"data","sentence_ids"};
        hBaseCRUDer.setTable(this.tableName);
    }

    public void migrate() {
        super.migrate();
        migrateFrequencies();
        migrateSentenceIds();
    }


    private void migrateFrequencies(){
        String query = "select " +
                "word as word " +
                "freq as freq " +
                "from words w";

        migrateTuple(query, "data", "freq", "long" );
    }

    private void migrateSentenceIds(){
        String query = "select " +
                "w.word as word " +
                "inv_w.s_id as s_id " +
                "from words w, inv_w " +
                "where w.w_id=inw_w.w_id";

        migrateTuple(query, "sentence_ids", "", "long");
    }

    public static void main(String[] args) {
        new WordEmigrationManager().migrate();
    }
}