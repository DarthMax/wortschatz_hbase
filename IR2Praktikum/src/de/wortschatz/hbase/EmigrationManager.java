package de.wortschatz.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.compiler.ir.Tuple;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Abstract class that manages the migration of a SQl database to an HBase table
 */
public abstract class EmigrationManager {

    /** An instance of HBaseCRUDer to manage HBase transactions */
    public HBaseCRUDer hBaseCRUDer;

    /** An instance of SqlDataGetter to manage retrieving Data from an SQL Database */
    public SqlDataGetter sqlDataGetter;

    /** The name of the HBase table the Data will be migrated to */
    protected String tableName;

    /** List of column families of the HBase table */
    protected String[] columnFamilies;


    /**
     * Instantiate a new EmigrationManager, creating new HBaseCRUDer and SqlDataGetter
     */
    public EmigrationManager() {
        this.hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        this.sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
    }

    /**
     * Create the HBase table with {@code tablename} and all column families {@code columnFamilies}
     */
    public void createTable() {
        System.out.println(this.tableName);

        HTableDescriptor tableDescriptor = new
        HTableDescriptor(TableName.valueOf(this.tableName));

        for(String columnFamily:this.columnFamilies) {
            tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
        }
        try {
            HBaseAdmin admin = new HBaseAdmin(HBaseConnector.get_connection());
            admin.createTable(tableDescriptor);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * A method to wrap all migration steps after creating the table
     */
    public void migrate() {
        createTable();
    }

    /**
     * Migrate a list of data tuples from SQL DB to HBase. This is done by splitting the data into chunks of 10000 rows.
     * @param query Query to return a list of data tuples. The first two selected values are stored in each tuple
     * @param column_family The column family where tuple data will be stored
     * @param qualifier The column qualifier, If blank the first tuple value will be used (must be of type {@code String})
     * @param value_class The class the second value whill be cast to (string, int, float, long)
     */
    protected void migrateTuple(String query, String column_family, String qualifier, String value_class){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());
        ArrayList<Tuple<Object,Object>> tuples;
        int offset = 0;
        int limit = 10000;


        do {
            String limited_query = query + " limit " + limit + " offset " + offset;
            System.out.println("limited_query = " + limited_query);
            tuples = dataGetter.getTuples(limited_query);

            if (!tuples.isEmpty()) {
                ArrayList<Put> putlist = new ArrayList<>();
                for (Tuple<Object,Object>  tuple: tuples) {
                    byte[] key = Bytes.toBytes((String) tuple.a);
                    Put put = new Put(key);

                    if(!qualifier.equals("")) {
                        switch (value_class) {
                            case "int":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes(qualifier), Bytes.toBytes((Integer) tuple.b));
                                break;
                            case "float":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes(qualifier), Bytes.toBytes((Float) tuple.b));
                                break;
                            case "long":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes(qualifier), Bytes.toBytes((Long) tuple.b));
                                break;
                            default:
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes(qualifier), Bytes.toBytes((String) tuple.b));
                                break;
                        }
                    }else {
                        switch (value_class) {
                            case "int":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes((Integer) tuple.b), Bytes.toBytes((Integer) tuple.b));
                                break;
                            case "float":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes((Float) tuple.b), Bytes.toBytes((Float) tuple.b));
                                break;
                            case "long":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes((Long) tuple.b), Bytes.toBytes((Long) tuple.b));
                                break;
                            default:
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes((String) tuple.b), Bytes.toBytes((String) tuple.b));
                                break;
                        }
                    }

                    putlist.add(put);
                }
                hBaseCRUDer.updateTable(putlist);
            }
            offset += limit;
        } while(!tuples.isEmpty());
    }
}
