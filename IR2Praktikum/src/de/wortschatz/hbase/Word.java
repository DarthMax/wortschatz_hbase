package de.wortschatz.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by max on 24.01.15.
 */
public class Word {
    private String value;
    private Long frequency;
    private HashSet<Integer> sentenceIDs;

    public String getValue() {
        return value;
    }

    public HashSet<Integer> getSentenceIDs() {
        return sentenceIDs;
    }

    public Long getFrequency() {
        return frequency;
    }

    public Word(String value, Long frequency) {
        this.value = value;
        this.frequency = frequency;

        setSentenceIDs();
    }




    private void setSentenceIDs() {
        String query = "select " +
                "inv_w.s_id as sentence_id " +
                "from words w, inv_w " +
                "where words.word='"+this.value+" " +
                "and word.id=inv_w.w_id";

        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());

        ArrayList<HashMap<String,Object>> rows = dataGetter.getDataFromQuery(query);
        HashSet<Integer> sentence_ids = new HashSet<>();

        for(HashMap<String,Object> row : rows) {
            sentence_ids.add((int) row.get("sentence_id"));
        }

        this.sentenceIDs = sentence_ids;
    }
}
