package de.wortschatz.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by max on 24.01.15.
 */
public class Source {

    private String value;
    private HashSet<Integer> sentences;

    public String getValue() {
        return value;
    }

    public HashSet<Integer> getSentences() {
        return sentences;
    }

    public Source(String value) {

        this.value = value;
    }

    private void setSentences() {
        String query = "select " +
                "inv_so.s_id as id" +
                "from inv_so, sources" +
                "where sources.source='"+this.value+" " +
                "and s.id=inv_so.so_id";

        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());

        ArrayList<HashMap<String,Object>> rows = dataGetter.getDataFromQuery(query);
        HashSet<Integer> sentences = new HashSet<>();

        for(HashMap<String,Object> row : rows) {
            sentences.add((Integer) row.get("id"));
        }

        this.sentences = sentences;
    }
}
