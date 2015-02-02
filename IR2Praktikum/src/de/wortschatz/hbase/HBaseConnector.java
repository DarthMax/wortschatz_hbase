package de.wortschatz.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Singelton class that provides a connection to an HBase instance
 */
public class HBaseConnector{

    /**
     * The singelton instance
     */
    protected static HBaseConnector connection;

    /**
     * Get the HBase configuration instance
     * @return The HBase configuration instance
     */
    public static Configuration getConnection() {
        if (HBaseConnector.connection==null) {
            HBaseConnector.connection = new HBaseConnector();
        }
        return HBaseConnector.connection.conf;
    };

    private Configuration conf;

    /**
     *
     */
    public HBaseConnector() {
        this.conf = HBaseConfiguration.create();
        this.conf.setQuietMode(true);
    }
}