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

        sqlDataGetter = new SqlDataGetter(SqlConnector.getConnection());
        hBaseCRUDer = new HBaseCRUDer(HBaseConnector.getConnection());
        String postfix = HBaseProploader.getProperties().getProperty("tablePostfix");
        hBaseCRUDer.setTable("cooccurrences"+postfix);
    }

    /**
     *
     * @param word
     * @return
     */
    public HashMap<String, Object> getMysqlPerformance(String word) {
        HashMap<String, Object> returnData = new HashMap<String, Object>();
        double startTime=0;
        startTime = System.currentTimeMillis();
        int resultSize = sqlDataGetter.getCooccurrenceData(word).size();
        double execTime = (System.currentTimeMillis()-startTime)*1.0/1000;
        returnData.put("execTime", execTime);
        returnData.put("resultSize", resultSize);
        return returnData ;
    }

    /**
     *
     * @param startRow
     * @return
     */
    public HashMap<String, Object> getHBasePerformance(String startRow) {
        HashMap<String, Object> returnData = new HashMap<String, Object>();
        double startTime;
        startTime = System.currentTimeMillis();
        int resultSize = hBaseCRUDer.convertToCooccurrences(hBaseCRUDer.scanTable(hBaseCRUDer.getScan(startRow))).size();
        double execTime = (System.currentTimeMillis()-startTime)*1.0/1000;
        returnData.put("execTime", execTime);
        returnData.put("resultSize", resultSize);
        return returnData ;
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
     * @param rows
     * @param fileName
     */
    public static void mapToCSV(ArrayList<HashMap<String, Object>> rows,String fileName) {
        try {
            File f = new File(fileName);
            if(f.exists() && !f.isDirectory()) { //file exists
                CSVPrinter printer = new CSVPrinter(new PrintWriter(f), CSVFormat.DEFAULT);
                printer.printRecords(rows);
                printer.close();
            } else { //new file
                CSVPrinter printer = new CSVPrinter(new PrintWriter(f), CSVFormat.DEFAULT);
                printer.printRecords(rows);
                printer.close();
            }




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
        ArrayList<HashMap<String, Object>> csvData = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            System.out.println(i+" seedSize = " + seedSize);
            randWords = pt.getWordSeed(seedSize);
            double hbaseTime = 0;
            double mysqlTime = 0;
            int hbaseNumberOfNullRequest = 0;
            int mysqlNumberOfNullRequest = 0;
            double hbaseNumberOfNullRequestTime = 0;
            double mysqlNumberOfNullRequestTime = 0;
            for (String randWord : randWords) {
                HashMap<String,Object> hbaseResult = pt.getHBasePerformance(randWord);
                HashMap<String,Object> mysqlResult = pt.getMysqlPerformance(randWord);
                mysqlTime += (double) mysqlResult.get("execTime");
                hbaseTime += (double) hbaseResult.get("execTime");
                if ((int) mysqlResult.get("resultSize")==0) {
                    mysqlNumberOfNullRequest += 1;
                    mysqlNumberOfNullRequestTime += mysqlTime;
                }
                if ((int) hbaseResult.get("resultSize")==0) {
                    hbaseNumberOfNullRequest += 1;
                    hbaseNumberOfNullRequestTime += hbaseTime;
                }
            }
            System.out.println(i + " seedSize = " + seedSize + " DONE");
            HashMap<String, Object> mysqlData = new HashMap<String, Object>();
            mysqlData.put("dbase", "Mysql"+tblStr);
            mysqlData.put("runtime", mysqlTime);
            mysqlData.put("nullReq", mysqlNumberOfNullRequest);
            mysqlData.put("nullReqTime", mysqlNumberOfNullRequestTime);
            HashMap<String, Object> hbaseData = new HashMap<String, Object>();
            hbaseData.put("dbase", "HBase"+tblStr);
            hbaseData.put("nullReq", hbaseNumberOfNullRequest);
            hbaseData.put("runtime", hbaseTime);
            hbaseData.put("nullReqTime", hbaseNumberOfNullRequestTime);
            csvData.add(mysqlData);
            csvData.add(hbaseData);
        }
        mapToCSV(csvData, tblStr+"Perf.csv");
//        mapToCSV(perfDataHbase, tblStr+"HbasePerf.csv");

        System.out.println(" DONE");
    }
}
