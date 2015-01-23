package de.wortschatz.hbase;

import org.apache.hadoop.hbase.client.Put;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Organizes the Migration of Wortschatz database from mysql to Hbase
 * Created by Marcel Kisilowski  on 22.01.15.
 * Author: Marcel Kisilowski, Max Kießling, Wolfgang Otto
 */
public class EmigrationManager {
    public HBaseCRUDer hBaseCRUDer;
    public SqlDataGetter sqlDataGetter;

    public final String tableName = "cooccurrences";
    private final String columnFamily = "data";
    private final String columnQualifier = "qual";
    private final String value = "val";


    public EmigrationManager() {
        this.hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        try {
            this.sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    //private void

    /**
     * @todo make a mthod called createPuts in Class hBaseCRUDer??
     * @param rows
     */
    public void putData(List<String> rows) {
        hBaseCRUDer.setTable(tableName);
        List<Put> putList = new ArrayList<>();
        for (String key : rows) {
            putList.add(hBaseCRUDer.createPut(key,columnFamily,columnQualifier,value));
        }
        hBaseCRUDer.updateTable(putList);
    }
    public static void main(String[] args){
        String query = "select w1.word, 'l', 1-c.sig/1000000, w2.word from words w1, words w2, co_s c " +
                "where w1.w_id=c.w1_id and c.w2_id=w2.w_id";
        EmigrationManager e = new EmigrationManager();
        e.hBaseCRUDer.setTable(e.tableName);
        e.putData(e.sqlDataGetter.getDataFromQuery(query));

    }
}
