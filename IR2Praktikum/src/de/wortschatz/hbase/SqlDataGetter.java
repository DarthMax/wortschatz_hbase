package de.wortschatz.hbase;


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


    public ArrayList<Cooccurrence> getCooccurrenceData(String type,int offset,int limit){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());
        String query = "Select " +
                "w1.word as word1, " +
                "w2.word as word2, " +
                "c.sig as sig, " +
                "c.freq as freq, " +
                "from words w1, words w2, "+type+" c " +
                "where w1.w_id=c.w1_id and c.w2_id=w2.w_id " +
                "limit"+limit+" "+
                "offset"+offset+" ";

        ArrayList<HashMap<String,Object>> rows = dataGetter.getDataFromQuery(query);

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

