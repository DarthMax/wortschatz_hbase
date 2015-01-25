package de.wortschatz.hbase;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Created by Marcel Kisilowski on 24.01.15.
 */
public class PerformanceTests {
    SqlDataGetter sqlDataGetter;
    HBaseCRUDer hBaseCRUDer;
    public PerformanceTests() {
        sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
        hBaseCRUDer = new HBaseCRUDer(HBaseConnector.get_connection());
        hBaseCRUDer.setTable("cooccurrences1M");
    }

    public double getMysqlPerformance(String word) {
        double startTime=0;
        startTime = System.currentTimeMillis();
        sqlDataGetter.getCooccurrenceData(word);
        return (System.currentTimeMillis()-startTime)*1.0/ 1000;
    }

    public double getHBasePerformance(String startRow) {
        double startTime;
        startTime = System.currentTimeMillis();
        hBaseCRUDer.convertToCooccurrences(hBaseCRUDer.scanTable(hBaseCRUDer.getScan(startRow)));
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

    public static void mapToCSV(TreeMap<Integer,ArrayList<Double>> map,String fileName) {
        try {
            CSVPrinter printer = new CSVPrinter(new PrintWriter(fileName), CSVFormat.DEFAULT);
            for(Integer seedSize:map.keySet()) {
                for(Double time : map.get(seedSize))
                    printer.printRecord(seedSize,time);
            }
            printer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        PerformanceTests pt = new PerformanceTests();
        ArrayList<String> randWords;
        TreeMap<Integer,ArrayList<Double>> perfDataMysql = new TreeMap<>();
        TreeMap<Integer,ArrayList<Double>> perfDataHbase = new TreeMap<>();
        for(int iterations=1;iterations<=1000;iterations*=10) {
            System.out.println("iterations = " + iterations);
            for (int seedSize = 1; seedSize <= 1000; seedSize *= 10) {
                ArrayList<Double> hbaseTimeCompl = new ArrayList<>();
                ArrayList<Double> mysqlTimeCompl = new ArrayList<>();
                for (int i = 0; i < iterations; i++) {
                    System.out.println(i+" seedSize = " + seedSize);
                    randWords = pt.getWordSeed(seedSize);
                    double hbaseTime = 0;
                    double mysqlTime = 0;
                    for (String randWord : randWords) {
                        mysqlTime += pt.getMysqlPerformance(randWord);
                        hbaseTime += pt.getHBasePerformance(randWord);
                    }
                    System.out.println(i+" seedSize = " + seedSize + " DONE");
                    hbaseTimeCompl.add(hbaseTime);
                    mysqlTimeCompl.add(mysqlTime);
                }
                perfDataMysql.put(seedSize, mysqlTimeCompl);
                perfDataHbase.put(seedSize, hbaseTimeCompl);
            }
            System.out.println("iterations = " + iterations + " DONE");
            mapToCSV(perfDataMysql, iterations + "iterationsMysqlPerf.csv");
            mapToCSV(perfDataHbase, iterations + "iterationsHbasePerf.csv");
        }
    }
}
