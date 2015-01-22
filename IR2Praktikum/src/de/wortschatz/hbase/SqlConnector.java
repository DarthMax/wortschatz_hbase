package de.wortschatz.hbase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class SqlConnector{

    protected static SqlConnector connection;

    public static Connection get_connection() throws SQLException {
        if (SqlConnector.connection==null) {
            SqlConnector.connection = new SqlConnector();
        }
        return SqlConnector.connection.conn;
    };

    public static void close() {
        try {
            SqlConnector.get_connection().close();
        } catch (SQLException e) {
            System.out.println("Could not close Connection");
        }
        SqlConnector.connection = null;
    }




    private Connection conn;

    private SqlConnector() throws SQLException{
        this.conn = DriverManager.getConnection("jdbc:mysql://localhost/test?" +
                                   "user=monty&password=greatsqldb");
    };

    public void finalize() {
        if(!(this.conn==null)){
            try {
                this.conn.close();
            } catch (SQLException e) {
                System.out.println("Could not close connection!");
            }
        }
    };
}