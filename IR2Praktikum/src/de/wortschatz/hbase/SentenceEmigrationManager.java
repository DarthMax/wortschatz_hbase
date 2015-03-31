package de.wortschatz.hbase;

/**
 * Emigration manager that migrates the sentence data from SQL to HBase.
 * The row key is the sentence id extracted from the SQL database.
 * The data column family stores the sentence in the value column.
 * The words column family stores every word of the sentence.
 * The sources column family stores every source url for a sentence.
 */
public class SentenceEmigrationManager extends EmigrationManager {
    public SentenceEmigrationManager(){
        this.tableName = "sentences"+HBaseProploader.getProperties().getProperty("tablePostfix");;
        this.columnFamilies = new String[]{"data", "words", "sources"};
        hBaseCRUDer.setTable(this.tableName);
    }

    public void migrate() {
        super.migrate();
        migrateSentences();
        migrateWords();
        migrateSources();
    }

    /**
     * Migrate the sentence string.
     */
    private void migrateSentences() {
        String query = "select " +
                "CAST(s_id as char), " +
                "sentence " +
                "from sentences";
        migrateTuple(query, "data", "value", "string");
    }

    /**
     * Migrate the word sentence connections creating a column for every word connected to the sentence.
     */
    private void migrateWords(){
        String query = "select " +
                "CAST(inv_w.s_id as char) as s_id, " +
                "w.word as word " +
                "from words w, inv_w " +
                "where w.w_id=inv_w.w_id";
        migrateTuple(query, "words", "", "string");
    }

    /**
     * Migrate the sentence source connections creating a column for every URL.
     */
    private void migrateSources(){
        String query = "select " +
                "CAST(inv_so.s_id as char) as s_id, " +
                "sources.source as source " +
                "from sources, inv_so " +
                "where sources.so_id=inv_so.so_id";
        migrateTuple(query, "sources", "", "string");
    }
}
