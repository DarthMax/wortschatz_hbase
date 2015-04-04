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
        boolean migrationWasValid = getHBaseCooccurrences()==getMysqlCooccurrences();
        return migrationWasValid;
    }
    private long getHBaseCooccurrences() {
        HBaseCRUDer hbase = new HBaseCRUDer(HBaseConnector.getConnection());
        long rows = hbase.countRowsInTable("cooccurrences"+HBaseProploader.getProperties().getProperty("tablePostfix"));
        return rows;
    }
    private long getMysqlCooccurrences() {
        SqlDataGetter sql = new SqlDataGetter(SqlConnector.getConnection());
        String query = "select (select count(*) from co_n)+(select count(*) from co_s)";
        ArrayList<HashMap<String, Object>> result = sql.getDataFromQuery(query);
        long rows = 0L;
        for (HashMap<String, Object> stringObjectHashMap : result) {
            for (String s : stringObjectHashMap.keySet()) {
                rows = (long) stringObjectHashMap.get(s);
                System.out.println("Result: "+stringObjectHashMap.get(s));
            }
        }
        return rows;
    }
}
