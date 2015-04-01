package de.wortschatz.hbase;

/**
 * Generates the row key for the cooccurrence table
 */
public class CooccurrenceKeyGenerator {

    /** Stores value used to normalize all significance values to be lower of equal than 1 */
    private Float norm;

    /** Stores the name of the cooccurrence type */
    private String type;

    /**
     * Create a new key generator for the given cooccurrence type
     * @param type The cooccurrence type specified by the table name
     */
    public CooccurrenceKeyGenerator(String type) {
        this.type = type;
        setNorm();
    }

    /**
     * Generate the row key for the given cooccurrence object.
     * @param co The cooccurrence object the row key is generated for
     * @return The row key for the cooccurrence following the pattern word1|type|inv_sig|word2
     */
    public String keyFor(Cooccurrence co) {
        return  co.getWord1()+"|"
               +this.type+"|"
               +Float.toString(normalizedSignificance(co))+"|"
               +co.getWord2();
    }

    /**
     * Set the value used for normalizing the significance value to be <= 1
     */
    private void setNorm(){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.getConnection());
        String query = "SELECT MAX(sig) as 'max' from "+type+";";

        norm =  (Float) dataGetter.getDataFromQuery(query).get(0).get("max");
    }

    /**
     * Normalize the significance value to be <=1 and inverse it
     * @param co The cooccurrence which significance should be normalized
     * @return The normalized, inverted significance
     */
    private Float normalizedSignificance(Cooccurrence co) {
       return  1-co.getSignificance()/norm;
    }
}
