package de.wortschatz.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseConnector{

    protected static HBaseConnector connection;

    public static Configuration get_connection() {
        if (HBaseConnector.connection==null) {
            HBaseConnector.connection = new HBaseConnector();
        }
        return HBaseConnector.connection.conf;
    };


    private Configuration conf;

    public HBaseConnector() {
        this.conf = HBaseConfiguration.create();
        this.conf.setQuietMode(true);
    }
}