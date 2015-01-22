package de.wortschatz.hbase;


import java.sql.*;
import java.util.ArrayList;


/**
 * Get Data from defined MySQL Database and arrange them for HBase
 * Created by wolfo on 15.01.15.
 */
public class SqlDataGetter {


    private Connection con = null;

    public SqlDataGetter(){}

    public SqlDataGetter(Connection con) {
        this.con = con;
    }



    public ArrayList<String> getDataFromQuery(String query) {
        String separator = "|";
        return getDataFromQuery(query, separator);
    }
    /**
     * Method to get ansewer for sql query as String
     * @param query valid sql query
     * @return answer as String
     */
    public ArrayList<String> getDataFromQuery(String query, String separator) {
        ArrayList<String> resultList = new ArrayList<String>();
        try
        {
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int nrOfColumns = rsmd.getColumnCount();
            while (resultSet.next())
            {
                String oneRow = "";
                for (int oneColumn=1;oneColumn<=nrOfColumns;oneColumn++) {
                    if (oneColumn != 1) oneRow += separator;
                    oneRow += resultSet.getString(oneColumn);
                }
                resultList.add(oneRow);
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return resultList;
    }
}

