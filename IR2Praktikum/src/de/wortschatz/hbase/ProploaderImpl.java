package de.wortschatz.hbase;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Marcel Kisilowski on 31.03.15.
 */

/**
 * Load props from Prop-file
 */
public class ProploaderImpl implements Proploader {
    /**
     * Load props from Prop-file
     * @param fileName
     * @return
     */
    @Override
    public Properties getProperties(String fileName) {
        String prefix = "";
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream(fileName);
            prop.load(input);
            input.close();

        } catch(FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("File not found.");
        }catch (IOException e) {
            e.printStackTrace();
            System.out.println("IOERROR ");
        }
        return prop;
    }
}
