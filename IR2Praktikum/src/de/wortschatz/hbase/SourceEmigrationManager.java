package de.wortschatz.hbase;

/**
 * Emigration manager that migrates the source data from SQL to HBase.
 * The row key is the source URL.
 * The sentence_ids column family stores the id of every sentence connected to the source
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

    /**
     * Migrate the sentence source connections by creating a column for every sentence connected to the source.
     */
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
