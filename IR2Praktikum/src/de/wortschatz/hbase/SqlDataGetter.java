package de.wortschatz.hbase;


import org.jruby.compiler.ir.Tuple;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;


/**
 * Get Data from defined MySQL Database and arrange them for HBase
 * Created by wolfo on 15.01.15.
 */
public class SqlDataGetter {


    private Connection con = null;

    public SqlDataGetter(Connection con) {
        this.con = con;
    }


    /**
     * Retrives a chunk of cooccurrence pairs from the database and turns them into Cooccurrence objects.
     * @param type The type of cooccurrences specified by the table name
     * @param offset The number of the start row
     * @param limit The number of rows to fetch from the database
     * @return A list of Cooccurrence objects
     */
    public ArrayList<Cooccurrence> getCooccurrenceData(String type,int offset,int limit){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.getConnection());
        String query = "Select " +
                "w1.word as word1, " +
                "w2.word as word2, " +
                "c.sig as sig, " +
                "c.freq as freq " +
                "from words w1 " +
                "join (select w_id from words limit "+limit+" offset "+offset+" ) as temp on temp.w_id=w1.w_id " +
                "inner join "+type+" c on w1.w_id=c.w1_id " +
                "inner join words w2 on c.w2_id=w2.w_id; ";

        System.out.println("query = " + query);

        ArrayList<HashMap<String,Object>> rows = getDataFromQuery(query);

        ArrayList<Cooccurrence> data = new ArrayList<>();

        for(HashMap<String,Object> row : rows) {
            Cooccurrence co = new Cooccurrence(
                    (String) row.get("word1"),
                    (String) row.get("word2"),
                    (Float) row.get("sig"),
                    (Long)  row.get("freq"));
            data.add(co);
        }

        System.out.println("data.size = " + data.size());

        return data;
    }
    public ArrayList<Cooccurrence> getCooccurrenceData(String searchWord){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.getConnection());
        String querySentence = "Select " +
                "w1.word as word1, " +
                "w2.word as word2, " +
                "c.sig as sig, " +
                "c.freq as freq " +
                "from words w1, words w2, co_s c " +
                "where w1.w_id=c.w1_id and c.w2_id=w2.w_id and w1.word=?";
        String queryNeighbour = "Select " +
                "w1.word as word1, " +
                "w2.word as word2, " +
                "c.sig as sig, " +
                "c.freq as freq " +
                "from words w1, words w2, co_n c " +
                "where w1.w_id=c.w1_id and c.w2_id=w2.w_id and w1.word=?";
        ArrayList<HashMap<String,Object>> rows = dataGetter.getDataFromQuery(querySentence,searchWord);
        rows.addAll(dataGetter.getDataFromQuery(queryNeighbour,searchWord));
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

    /**
     * Runs the query on the database and returns the first two values of each row in the result set as a tuple.
     * @param query The query to perform against the database. Must have at least two return values per row
     * @return A list of tuples. One for each row in the result set
     */
    public ArrayList<Tuple<Object,Object>> getTuples(String query) {
        try {
            ArrayList<Tuple<Object, Object>> tuples = new ArrayList<>();

            Connection con = SqlConnector.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                tuples.add(new Tuple<>(resultSet.getObject(1), resultSet.getObject(2)));
            }
            return tuples;
        } catch(SQLException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
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

