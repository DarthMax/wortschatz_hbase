package de.wortschatz.hbase.test;

import de.wortschatz.hbase.HBaseInstallation;
import junit.framework.TestCase;

public class HBaseInstallationTest extends TestCase {

    public void testCreateTables() throws Exception {
        assertEquals(true, HBaseInstallation.createTables("conf/hbasetables.json"));
    }
    public void testJsonToMap() throws Exception {
        assertNotSame(0, HBaseInstallation.jsonToMap("conf/hbasetables.json").size());
    }
}