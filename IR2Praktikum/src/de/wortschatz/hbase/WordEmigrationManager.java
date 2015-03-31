package de.wortschatz.hbase;

/**
 * Emigration manager that migrates the word data from SQL to HBase.
 * The row key is the word.
 * The data column family stores the frequency the words occurrences in the corpus.
 * The sentence_ids column family stores the ID of every sentence the word occurred in.
 */
public class WordEmigrationManager extends EmigrationManager{

    public WordEmigrationManager(){
        this.tableName = "words"+HBaseProploader.getProperties().getProperty("tablePostfix");;
        this.columnFamilies = new String[]{"data","sentence_ids"};
        hBaseCRUDer.setTable(this.tableName);
    }

    public void migrate() {
        super.migrate();
        migrateFrequencies();
        migrateSentenceIds();
    }

    /**
     * Migrate the frequency data for every word
     */
    private void migrateFrequencies(){
        String query = "select " +
                "word as word, " +
                "freq as freq " +
                "from words";
        migrateTuple(query, "data", "freq", "long" );
    }

    /**
     * Migrate the word sentence connections by creating a column for every sentence connected to the word.
     */
    private void migrateSentenceIds(){
        String query = "select " +
                "w.word as word, " +
                "inv_w.s_id as s_id " +
                "from words w, inv_w " +
                "where w.w_id=inv_w.w_id";

        migrateTuple(query, "sentence_ids", "", "long");
    }

    public static void main(String[] args) {
        new WordEmigrationManager().migrate();
    }
}