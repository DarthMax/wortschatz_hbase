package de.wortschatz.hbase;

import java.util.Properties;

/**
 * Created by Marcel Kisilowski on 31.03.15.
 */
public class MysqlProploader extends ProploaderImpl {
    private static final String propFilePath = System.getProperty("user.home") + "/.conf/sql2hbase/sql_database.properties";
    public static Properties getProperties() {
        return new MysqlProploader().getProperties(propFilePath);
    }
}
