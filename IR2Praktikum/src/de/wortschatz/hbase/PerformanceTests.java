package de.wortschatz.hbase;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Marcel Kisilowski on 24.01.15.
 */
public class PerformanceTests {
    SqlDataGetter sqlDataGetter;
    HBaseCRUDer hBaseCRUDer;
    public PerformanceTests() {
        sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
        hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        hBaseCRUDer.setTable("cooccurrences");
    }

    public double getMysqlPerformance(String query,String word) {
        double startTime=0;
        startTime = System.currentTimeMillis();
        sqlDataGetter.getDataFromQuery(query,word);
        return (System.currentTimeMillis()-startTime)*1.0/ 1000;
    }

    public double getHBasePerformance(String startRow) {
        double startTime;
        startTime = System.currentTimeMillis();
        hBaseCRUDer.scanTable(hBaseCRUDer.getScan(startRow));
        return (System.currentTimeMillis()-startTime)*1.0/1000 ;
    }

    public ArrayList<String> getWordSeed(int size) {
        String query = "SELECT word FROM words\n" +
                "ORDER BY RAND()\n" +
                "LIMIT "+size;
        ArrayList<String> result = new ArrayList<>();
        ArrayList<HashMap<String,Object>> sqlResult = sqlDataGetter.getDataFromQuery(query);
        for (HashMap<String, Object> sqlRes : sqlResult) {
            result.add(sqlRes.get("word").toString());
        }
        return result;
    }

    public static void main(String[] args) {
        PerformanceTests pt = new PerformanceTests();
        String query = "select w1.word, 'l', 1-c.sig/1000000, w2.word from words w1, words w2, co_s c " +
                "where w1.w_id=c.w1_id and c.w2_id=w2.w_id and w1.word=\"der\";";
        String startRow = "der";

        ArrayList<String> randWords;
        double hbaseTimeCompl=0;
        double mysqlTimeCompl=0;
        int iterations=100;
        int seedSize = 1000;
        for(int i = 0;i<iterations;i++) {
            randWords = pt.getWordSeed(seedSize);
            double hbaseTime=0;
            double mysqlTime=0;
            for (String randWord : randWords) {
                query = "select w1.word, 'l', 1-c.sig/1000000, w2.word from words w1, words w2, co_s c " +
                        "where w1.w_id=c.w1_id and c.w2_id=w2.w_id and w1.word=?";
                mysqlTime += pt.getMysqlPerformance(query,randWord);
                hbaseTime += pt.getHBasePerformance(randWord);
            }
            hbaseTimeCompl+=hbaseTime;
            mysqlTimeCompl+=mysqlTime;

        }
        System.out.println("mysqlTime = " + mysqlTimeCompl/iterations);
        System.out.println("hbaseTime = " + hbaseTimeCompl/iterations);
    }
}
