package de.wortschatz.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by max on 24.01.15.
 */
public class Sentence {

    private Integer id;
    private String text;
    private HashSet<String> words;
    private HashSet<String> sources;

    public Integer getId() {
        return id;
    }

    public String getText() {
        return text;
    }

    public HashSet<String> getWords() {
        return words;
    }

    public HashSet<String> getSources() {
        return sources;
    }

    public Sentence(Integer id, String text) {

        this.id = id;
        this.text = text;
        setSources();
        setWords();
    }

    private void setWords() {
        String query = "select " +
                "words.word" +
                "from words w, inv_w " +
                "where inv_w.s_id='"+this.id+" " +
                "and word.id=inv_w.w_id";

        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());

        ArrayList<HashMap<String,Object>> rows = dataGetter.getDataFromQuery(query);
        HashSet<String> words = new HashSet<>();

        for(HashMap<String,Object> row : rows) {
            words.add((String) row.get("word"));
        }

        this.words = words;
    }

    private void setSources() {
        String query = "select " +
                "s.source" +
                "from sources s, inv_so " +
                "where inv_s.s_id='"+this.id+" " +
                "and s.id=inv_so.so_id";

        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());

        ArrayList<HashMap<String,Object>> rows = dataGetter.getDataFromQuery(query);
        HashSet<String> sources = new HashSet<>();

        for(HashMap<String,Object> row : rows) {
            sources.add((String) row.get("source"));
        }

        this.sources = sources;
    }


}
