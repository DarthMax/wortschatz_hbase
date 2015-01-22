package de.wortschatz.hbase;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class SqlConnector{

    protected static SqlConnector connection;

    public static Connection get_connection() throws SQLException, ClassNotFoundException {
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

    private SqlConnector() throws SQLException, ClassNotFoundException {
        String dbClass      = "com.mysql.jdbc.Driver";
        String dbHost       = "localhost";
        String dbPort       = "3306";
        String dbName       = "test";
        String dbUser       = "user";
        String dbPassword   = "passwd";

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
        }

        Class.forName(dbClass);
        this.conn = DriverManager.getConnection("jdbc:mysql://"+dbHost+":"+ dbPort+"/"+dbName+"?"+"user="+dbUser+"&"+"password="+dbPassword);
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
        input = new FileInputStream("conf/sql_database.properties");
        prop.load(input);
        input.close();
        return prop;
    }
}