package de.wortschatz.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;

/**
 * Created by max on 25.01.15.
 */
public class FindMissingCooccurrences {


    public static void find_missing() {
        String type = "co_s";
        SqlDataGetter dataGetter = new SqlDataGetter(SqlConnector.get_connection());
        HBaseCRUDer hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        hBaseCRUDer.setTable("cooccurrences");
        CooccurrenceKeyGenerator generator = new CooccurrenceKeyGenerator(type);
        ArrayList<Cooccurrence> cos;
        int offset = 0;
        int limit = 10000;

        do {
            cos = dataGetter.getCooccurrenceData(type,offset,limit);

            if (!cos.isEmpty()) {
                for (Cooccurrence co : cos) {
                    String key = generator.keyFor(co);
                    Get get = new Get(Bytes.toBytes(key));
                    get.setFilter(new KeyOnlyFilter());
                    Result res = hBaseCRUDer.get(get);

                    if (res.isEmpty()) {
                        System.out.println("Key not found:"+key);
                    }

                }
            }
            offset += limit;
        } while(!cos.isEmpty());
    }

    public static void main(String[] args) {
        FindMissingCooccurrences.find_missing();
    }

}