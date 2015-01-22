package de.wortschatz.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Marcel Kisilowski on 20.01.15.
 */
public class HBaseAdminModel {

    private HTableDescriptor[] tableDescriptor;

    private HBaseAdmin admin;

    private HTable table;
    private Configuration conf;


    public HBaseAdminModel() throws IOException {
        conf = HBaseConfiguration.create();
        conf.setQuietMode(true);
        admin = new HBaseAdmin(conf);
        getTables();
    }

    public HBaseAdmin getAdmin() {
        return admin;
    }

    public HTableDescriptor[] getTableDescriptor() {
        return tableDescriptor;
    }

    public void getTables() {
        try {
            tableDescriptor = admin.listTables();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static void createDatabase(Configuration conf) {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);

			/*
			 * Pro Tabelle -->
			 */
            HTableDescriptor tableDescriptor = new HTableDescriptor(
                    TableName.valueOf("emp"));
            tableDescriptor.addFamily(new HColumnDescriptor("personal"));
            tableDescriptor.addFamily(new HColumnDescriptor("professional"));
            admin.createTable(tableDescriptor);
			/*
			 * <--
			 */
            System.out.println(" Tables created ");
            admin.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


    public void createTable(String tableName, ArrayList<String> colList) {
        HTableDescriptor tableDescriptor = new
                HTableDescriptor(TableName.valueOf(tableName));
        for (String column : colList) {
            tableDescriptor.addFamily(new HColumnDescriptor(column));
        }
        try {
            admin.createTable(tableDescriptor);
            getTables();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteTable(String tableName) {
        try {
            admin.deleteTable(tableName);
            getTables();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isTableEnabled(String tableName) {
        try {
            return admin.isTableEnabled(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public void switchTableStatusTo(String tableName, boolean status) {
        try {
            if (status) {
                admin.enableTable(tableName);
            } else {
                admin.disableTable(tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HTableDescriptor getTableDescriptor(String tableName) {
        try {
            return admin.getTableDescriptor(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> getColumnFamilies(String tableName) {
        List<HColumnDescriptor> result = Arrays.asList(getTableDescriptor(tableName).getColumnFamilies());
        List<String> list = new ArrayList<>();
        for (HColumnDescriptor hColumnDescriptor : result) {
            list.add(hColumnDescriptor.getNameAsString());
        }
        return list;
    }

    public boolean putRow(String tableName,String rowName,String columnFamily, String qulifier,String value){
        boolean ret = false;
        try {
            if(table != null && !table.getTableDescriptor().getNameAsString().equals(tableName)) {
                table = new HTable(conf,tableName);
            }
            Put put = new Put(Bytes.toBytes(rowName));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }
}
