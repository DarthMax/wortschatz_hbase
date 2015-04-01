package de.wortschatz.hbase.validation;

import de.wortschatz.hbase.*;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Marcel Kisilowski on 01.04.15.
 */
public class CooccurrenceValidationTest implements ValidationTest {
    @Override
    public boolean migrationWasValid() {

        return false;
    }
    private int getHBaseCooccurrences() {
        HBaseCRUDer hbase = new HBaseCRUDer(HBaseConnector.getConnection());
        hbase.countRowsInTable("cooccurrences"+HBaseProploader.getProperties().getProperty("tablePostfix"));
        return 0;
    }
    private int getMysqlCooccurrences() {
        SqlDataGetter sql = new SqlDataGetter(SqlConnector.getConnection());
        String query = "select (select count(*) from co_n)+(select count(*) from co_s)";
        ArrayList<HashMap<String, Object>> result = sql.getDataFromQuery(query);
        for (HashMap<String, Object> stringObjectHashMap : result) {
            for (String s : stringObjectHashMap.keySet()) {
                System.out.println("Result: "+stringObjectHashMap.get(s));
            }
        }
        return 0;
    }
}
