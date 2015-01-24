package de.wortschatz.hbase;

import java.util.HashMap;

/**
 * Created by max on 24.01.15.
 */
public class CooccurrenceKeyGenerator {

    private Float norm;
    private String type;

    public CooccurrenceKeyGenerator(String type) {
        this.type = type;
        this.norm = getNorm(type);
    }

    public String keyFor(Cooccurrence co) {
        return  co.getWord1()+"|"
               +this.type+"|"
               +Float.toString(normalizedSignificance(co))+"|"
               +co.getWord2();
    }

    private Float getNorm(String type){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());
        String query = "SELECT MAX(sig) as 'max' from "+type+";";

        return (Float) dataGetter.getDataFromQuery(query).get(0).get("max");
    }

    private Float normalizedSignificance(Cooccurrence co) {
       return  1-co.getSignificance()/norm;
    }
}
