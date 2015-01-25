package de.wortschatz.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.compiler.ir.Tuple;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by max on 25.01.15.
 */
public abstract class EmigrationManager {

    public HBaseCRUDer hBaseCRUDer;
    public SqlDataGetter sqlDataGetter;

    protected String tableName;
    protected String[] columnFamilies;

    public EmigrationManager() {
        this.hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        this.sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
    }

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

    public void migrate() {
        createTable();
    }

    protected void migrateTuple(String query, String column_family, String qualifier, String value_class){
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());
        ArrayList<Tuple<Object,Object>> tuples;
        int offset = 0;
        int limit = 10000;


        do {
            String limited_query = query + " limit " + limit + " offset " + offset;

            tuples = dataGetter.getTuples(limited_query);

            if (!tuples.isEmpty()) {
                ArrayList<Put> putlist = new ArrayList<>();
                for (Tuple<Object,Object>  tuple: tuples) {
                    byte[] key = Bytes.toBytes((String) tuple.a);
                    Put put = new Put(key);

                    if(qualifier!="") {
                        switch (value_class) {
                            case "int":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes(qualifier), Bytes.toBytes((Integer) tuple.b));
                            case "float":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes(qualifier), Bytes.toBytes((Float) tuple.b));
                            case "long":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes(qualifier), Bytes.toBytes((Long) tuple.b));
                            default:
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes(qualifier), Bytes.toBytes((String) tuple.b));
                        }
                    }else {
                        switch (value_class) {
                            case "int":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes((Integer) tuple.b), Bytes.toBytes((Integer) tuple.b));
                            case "float":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes((Float) tuple.b), Bytes.toBytes((Float) tuple.b));
                            case "long":
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes((Long) tuple.b), Bytes.toBytes((Long) tuple.b));
                            default:
                                put.add(Bytes.toBytes(column_family), Bytes.toBytes((String) tuple.b), Bytes.toBytes((String) tuple.b));
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
