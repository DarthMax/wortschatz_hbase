package de.wortschatz.hbase;

import java.util.Properties;

/**
 * Created by Marcel Kisilowski on 31.03.15.
 */
public interface Proploader {
    Properties getProperties(String fileName);
}
