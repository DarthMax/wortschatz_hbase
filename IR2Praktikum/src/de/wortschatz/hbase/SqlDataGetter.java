package de.wortschatz.hbase;





import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;


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

    protected get







    /**
     *
     */
    private String database = "jdbc:mysql://localhost/test";
    private String dbuser = "test";
    private String dbpassword = "test";
    private String dbClass = "com.mysql.jdbc.Driver";

    /**
     * getter
     * @return
     */
    public String getDatabase() {
        return database;
    }
    public String getDbpassword() {
        return dbpassword;
    }
    public String getDbuser() {
        return dbuser;
    }
    public String getDbClass() {
        return dbClass;
    }



    /**
     * CONSTUCTOR is loading config file
     */
    public SqlDataGetter() {
        // Load DB config from file
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream("conf/config.properties");
            // load a properties file
            prop.load(input);
            // get the property value and print it out
            database = prop.getProperty("database");
            dbuser = prop.getProperty("dbuser");
            dbpassword = prop.getProperty("dbpassword");
            dbClass = prop.getProperty("dbClass");
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        con = null;
    }

    /**
     * @return
     */
    String sayHello() {
        return "hello";
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
                    if (oneColum != 1) oneRow += separator;
                    oneRow += resultSet.getString(oneColumn);
                }
                resultList.add(oneRow);
            }
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return resultList;
    }
}

