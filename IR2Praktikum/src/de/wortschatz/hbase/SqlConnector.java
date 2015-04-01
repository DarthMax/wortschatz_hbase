package de.wortschatz.hbase;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Singelton class that provides a connection to an SQL database instance
 */
public class SqlConnector{

    /**
     * The singelton instance
     */
    protected static SqlConnector connection;

    /**
     * The location of the configuration file of the database connection
     */
    private final String propFilePath = System.getProperty("user.home") + "/.conf/sql2hbase/sql_database.properties";

    public static Connection getConnection() {
        if (SqlConnector.connection==null) {
            SqlConnector.connection = new SqlConnector();
        }
        return SqlConnector.connection.conn;
    }

    /**
     * Close the database connection
     */
    public static void close() {
        if (SqlConnector.connection!=null) {
            SqlConnector.connection.close();
        }
        SqlConnector.connection = null;
    }


    private Connection conn;


    /**
     * Establish a connection to the database using JDBC. Connection parameters can be configured using the connection
     * property file 'conf/sql_database.properties'
     */
    private SqlConnector() {
        String dbClass      = "com.mysql.jdbc.Driver";
        String dbHost       = "localhost";
        String dbPort       = "3306";
        String dbName       = "db";
        String dbUser       = "user";
        String dbPassword   = "password";

        try {
            Properties prop = connectionProperties();

            dbClass     = prop.getProperty("dbClass");
            dbHost      = prop.getProperty("dbHost");
            dbPort      = prop.getProperty("dbPort");
            dbName      = prop.getProperty("dbName");
            dbUser      = prop.getProperty("dbUser");
            dbPassword  = prop.getProperty("dbPassword");

        }catch (IOException e) {
            System.out.println("Could not read DB-Configuration. Using default");
            e.printStackTrace();
        }

        try {
            Class.forName(dbClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(42);
        }
        try {
            this.conn = DriverManager.getConnection("jdbc:mysql://"+dbHost+":"+ dbPort+"/"+dbName+"?"+"user="+dbUser+"&"+"password="+dbPassword);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(42);
        }
    }

    /**
     * Make sure the database connection is closed when the object is destroyed.
     */
    public void finalize() {
        if(!(this.conn==null)){
            try {
                this.conn.close();
            } catch (SQLException e) {
                System.out.println("Could not close connection!");
            }
        }
    }

    /**
     * Open and parse the database connection property file.
     * @return The database connection properties.
     * @throws IOException
     */
    private Properties connectionProperties() throws IOException {
        Properties prop = new Properties();
        InputStream input = null;
        input = new FileInputStream(propFilePath);
        prop.load(input);
        input.close();
        return prop;
    }
}