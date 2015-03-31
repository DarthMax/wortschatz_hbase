package de.wortschatz.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Singelton class that provides a connection to an HBase instance
 */
public class HBaseConnector{

    /**
     * The singelton instance
     */
    protected static HBaseConnector connection;

    /**
     * The location of the configuration file of the HBase connection
     */
    private final String propFilePath = System.getProperty("user.home") + "/.conf/sql2hbase/hbase.properties";

    /**
     * Get the HBase configuration instance
     * @return The HBase configuration instance
     */
    public static Configuration getConnection() {
        if (HBaseConnector.connection==null) {
            HBaseConnector.connection = new HBaseConnector();
        }
        return HBaseConnector.connection.conf;
    }

    private Configuration conf;

    /**
     * create a new Connection to the configured HBase server
     */
    public HBaseConnector() {
        String zooKeeperQuorum     = "localhost";
        String zooKeeperClientPort = "2181";

        try {
            Properties prop = connectionProperties();

            zooKeeperQuorum     = prop.getProperty("zooKeeperQuorum");
            zooKeeperClientPort = prop.getProperty("zooKeeperClientPort");
        }catch (IOException e) {
            System.out.println("Could not read HBase-Configuration. Using default");
            e.printStackTrace();
        }

        this.conf = HBaseConfiguration.create();
        this.conf.setQuietMode(true);
        this.conf.set("hbase.zookeeper.quorum", zooKeeperQuorum);
        this.conf.set("hbase.zookeeper.property.clientPort",zooKeeperClientPort);

    }


    /**
     * Open and parse the HBase connection property file.
     * @return The database connection properties.
     * @throws java.io.IOException
     */
    private Properties connectionProperties() throws IOException {
        Properties prop = new Properties();
        InputStream input = null;
        input = new FileInputStream(propFilePath);
        prop.load(input);
        input.close();
        return prop;
    }

    public static void main(String[] args) {
        Configuration conf = HBaseConnector.getConnection();

        try {
            HBaseAdmin.checkHBaseAvailable(conf);
            System.out.println("HBase is running");
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running!");
            System.exit(1);
        } catch (Exception ce) {
            ce.printStackTrace();
        }
    }
}