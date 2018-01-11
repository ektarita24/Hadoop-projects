package xyz.supercoders.bloghbase;

import static xyz.supercoders.bloghbase.Constants.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class GetAllPosts {
	
	public static void main(String[] args) {
		
		try {
			getAllPosts();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private static void getAllPosts() throws IOException {
		Configuration configuration = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf(TABLE_POSTS));
		
		Scan scan = new Scan();
		scan.addFamily(CF_INFO);
		ResultScanner rs = table.getScanner(scan);
		Result result = rs.next();
		
		while(result!=null){
			System.out.println(Bytes.toLong(result.getRow())+" "+Bytes.toString(result.getValue(CF_INFO, C_TITLE)));
			result = rs.next();
		}
		rs.close();
		table.close();
		connection.close();
	}
}
