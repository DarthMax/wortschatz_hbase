package de.wortschatz.hbase;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class SqlConnector{

    protected static SqlConnector connection;
    private final String propFilePath = System.getProperty("user.dir") + "/conf/sql_database.properties";

    public static Connection get_connection() {
        if (SqlConnector.connection==null) {
            SqlConnector.connection = new SqlConnector();
        }
        return SqlConnector.connection.conn;
    };

    public static void close() {
        if (SqlConnector.connection!=null) {
            SqlConnector.connection.close();
        }
        SqlConnector.connection = null;
    }


    private Connection conn;

    private SqlConnector() {
        String dbClass      = "com.mysql.jdbc.Driver";
        String dbHost       = "localhost";
        String dbPort       = "3306";
        String dbName       = "db";
        String dbUser       = "user";
        String dbPassword   = "password";

        try {
            Properties prop = connectionPropoerties();

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

    public void finalize() {
        if(!(this.conn==null)){
            try {
                this.conn.close();
            } catch (SQLException e) {
                System.out.println("Could not close connection!");
            }
        }
    }


    private Properties connectionPropoerties() throws IOException {
        Properties prop = new Properties();
        InputStream input = null;
        input = new FileInputStream(propFilePath);
        prop.load(input);
        input.close();
        return prop;
    }
}