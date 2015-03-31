package de.wortschatz.hbase;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by Marcel Kisilowski on 24.01.15.
 */
public class PerformanceTests {

    SqlDataGetter sqlDataGetter;
    HBaseCRUDer hBaseCRUDer;

    /**
     *
     */
    public PerformanceTests() {

        sqlDataGetter = new SqlDataGetter(SqlConnector.get_connection());
        hBaseCRUDer = new HBaseCRUDer(HBaseConnector.getConnection());
        String postfix = HBaseProploader.getProperties().getProperty("tablePostfix");
        hBaseCRUDer.setTable("cooccurrences"+postfix);
    }

    /**
     *
     * @param word
     * @return
     */
    public double getMysqlPerformance(String word) {
        double startTime=0;
        startTime = System.currentTimeMillis();
        int resultSize = sqlDataGetter.getCooccurrenceData(word).size();
        double execTime = (System.currentTimeMillis()-startTime)*1.0/1000;
        System.out.println("MYSQL result size: " + resultSize);
        System.out.println("MYSQL execTime: " + execTime);
        return execTime ;
    }

    /**
     *
     * @param startRow
     * @return
     */
    public double getHBasePerformance(String startRow) {
        double startTime;
        startTime = System.currentTimeMillis();
        int resultSize = hBaseCRUDer.convertToCooccurrences(hBaseCRUDer.scanTable(hBaseCRUDer.getScan(startRow))).size();
        double execTime = (System.currentTimeMillis()-startTime)*1.0/1000;
        System.out.println("HBASE result size: " + resultSize);
        System.out.println("HBASE execTime: " + execTime);
        return execTime ;
    }

    /**
     *
     * @param size
     * @return
     */
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

    /**
     *
     * @param map
     * @param fileName
     */
    public static <I,D> void mapToCSV(TreeMap<I,ArrayList<D>> map,String fileName) {
        try {
            CSVPrinter printer = new CSVPrinter(new PrintWriter(new FileWriter(fileName,true)), CSVFormat.DEFAULT);
            for(I seedSize:map.keySet()) {
                for(D time : map.get(seedSize))
                    printer.printRecord(seedSize,time);
            }
            printer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        PerformanceTests pt = new PerformanceTests();
        ArrayList<String> randWords;
        int iterations = 100;
        String tblStr = HBaseProploader.getProperties().getProperty("tablePostfix");
        System.out.println("iterations = " + iterations);
        int seedSize = 1000;
        TreeMap<Integer,ArrayList<Double>> perfDataMysql = new TreeMap<>();
        TreeMap<Integer,ArrayList<Double>> perfDataHbase = new TreeMap<>();
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
        mapToCSV(perfDataMysql, tblStr+"MysqlPerf.csv");
        mapToCSV(perfDataHbase, tblStr+"HbasePerf.csv");

        System.out.println(" DONE");
    }
}
