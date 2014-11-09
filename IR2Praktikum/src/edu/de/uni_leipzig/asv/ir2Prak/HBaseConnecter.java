package edu.de.uni_leipzig.asv.ir2Prak;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class HBaseConnecter {

	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTable table = new HTable(conf, "test");
			Result rs = table.get(new Get("row1".getBytes()));
			System.out.println("Connected");
			for (KeyValue kv : rs.raw()) {
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.println(new String(kv.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
