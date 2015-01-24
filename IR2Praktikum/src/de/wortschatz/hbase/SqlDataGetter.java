package de.wortschatz.hbase;


import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


/**
 * Get Data from defined MySQL Database and arrange them for HBase
 * Created by wolfo on 15.01.15.
 */
public class SqlDataGetter {


    private Connection con = null;

    public SqlDataGetter(Connection con) {
        this.con = con;
    }


    public ArrayList<Cooccurrence> getCooccurrenceData(String type,int offset,int limit){
        String query = "Select " +
                "w1.word as word1, " +
                "w2.word as word2, " +
                "c.sig as sig, " +
                "c.freq as freq, " +
                "from words w1, words w2, "+type+" c " +
                "where w1.w_id=c.w1_id and c.w2_id=w2.w_id " +
                "limit"+limit+" "+
                "offset"+offset+" ";

        ArrayList<HashMap<String,Object>> rows = getDataFromQuery(query);

        ArrayList<Cooccurrence> data = new ArrayList<>();

        for(HashMap<String,Object> row : rows) {
            Cooccurrence co = new Cooccurrence(
                    (String) row.get("word1"),
                    (String) row.get("word1"),
                    (Float) row.get("sig"),
                    (Long)  row.get("freq"));
            data.add(co);
        }

        return data;
    }

    public ArrayList<Word> getWords(int offset, int limit) {
        String query = "select word, freq from words" +
                " limit " + limit +
                " offset " + offset;

        ArrayList<HashMap<String,Object>> rows = getDataFromQuery(query);
        ArrayList<Word> words = new ArrayList<>();

        for(HashMap<String,Object> row : rows) {
            words.add(new Word((String) row.get("word"), (Long) row.get("freq")));
        }

        return words;
    }

    public ArrayList<Sentence> getSentences(int offset, int limit) {
        String query = "select text, id from sentences" +
                " limit " + limit +
                " offset " + offset;

        ArrayList<HashMap<String,Object>> rows = getDataFromQuery(query);
        ArrayList<Sentence> sentences = new ArrayList<>();

        for(HashMap<String,Object> row : rows) {
            sentences.add(new Sentence((Integer) row.get("id"), (String) row.get("text")));
        }

        return sentences;
    }


    public ArrayList<Source> getSources(int offset, int limit) {
        String query = "select source from sources" +
                " limit " + limit +
                " offset " + offset;

        ArrayList<HashMap<String,Object>> rows = getDataFromQuery(query);
        ArrayList<Source> sources = new ArrayList<>();

        for(HashMap<String,Object> row : rows) {
            sources.add(new Source((String) row.get("source")));
        }

        return sources;
    }



    /**
     * Method to get ansewer for sql query as String
     * @param query valid sql query
     * @return answer as ArrayList of HashMaps
     */
    public ArrayList<HashMap<String, Object>> getDataFromQuery(String query) {
        ArrayList<HashMap<String, Object>> resultList = new ArrayList<>();
        try
        {
            Statement statement = con.createStatement();

            ResultSet resultSet = statement.executeQuery(query);
            ResultSetMetaData rsmd = resultSet.getMetaData();

            int nrOfColumns = rsmd.getColumnCount();
            while (resultSet.next())
            {
                HashMap<String,Object> row = new HashMap<>();
                for (int col=1;col<=nrOfColumns;col++) {
                    row.put(rsmd.getColumnLabel(col), resultSet.getObject(col));
                }

                resultList.add(row);
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return resultList;
    }

    /**
     * Method to get ansewer for sql query as String
     * @param query valid sql query
     * @return answer as ArrayList of HashMaps
     */
    public ArrayList<HashMap<String, Object>> getDataFromQuery(String query,String word) {
        ArrayList<HashMap<String, Object>> resultList = new ArrayList<>();
        try
        {
            PreparedStatement statement = con.prepareStatement(query);
            statement.setString(1,word);

            ResultSet resultSet = statement.executeQuery();
            ResultSetMetaData rsmd = resultSet.getMetaData();

            int nrOfColumns = rsmd.getColumnCount();
            while (resultSet.next())
            {
                HashMap<String,Object> row = new HashMap<>();
                for (int col=1;col<=nrOfColumns;col++) {
                    row.put(rsmd.getColumnLabel(col), resultSet.getObject(col));
                }

                resultList.add(row);
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return resultList;
    }

}

