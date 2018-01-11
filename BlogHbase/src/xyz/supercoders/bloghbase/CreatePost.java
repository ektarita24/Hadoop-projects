package xyz.supercoders.bloghbase;

import static xyz.supercoders.bloghbase.Constants.CF_DETAILS;
import static xyz.supercoders.bloghbase.Constants.CF_INFO;
import static xyz.supercoders.bloghbase.Constants.C_AUTHOR;
import static xyz.supercoders.bloghbase.Constants.C_DATE;
import static xyz.supercoders.bloghbase.Constants.C_DESCRIPTION;
import static xyz.supercoders.bloghbase.Constants.C_TITLE;
import static xyz.supercoders.bloghbase.Constants.TABLE_POSTS;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class CreatePost {

	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		
		System.out.print("Enter title : ");
		String title = sc.nextLine();
		
		System.out.print("Enter author : ");
		String author = sc.nextLine();
		
		System.out.print("Enter description : ");
		String description = sc.nextLine();
		
		sc.close();
		
		try {
			insertPost(title,author,description);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void insertPost(String title, String author, String description) throws IOException {
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		String date = sdf.format(calendar.getTime());
		
		Configuration configuration = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf(TABLE_POSTS));
		
		Put put = new Put(Bytes.toBytes(buildRowKey()));
		put.addColumn(CF_INFO, C_TITLE, Bytes.toBytes(title));
		put.addColumn(CF_INFO, C_DATE, Bytes.toBytes(date));
		put.addColumn(CF_DETAILS, C_TITLE, Bytes.toBytes(title));
		put.addColumn(CF_DETAILS, C_DATE, Bytes.toBytes(date));
		put.addColumn(CF_DETAILS, C_AUTHOR, Bytes.toBytes(author));
		put.addColumn(CF_DETAILS, C_DESCRIPTION, Bytes.toBytes(description));
		
		table.put(put);
		table.close();
		connection.close();
	}

	private static long buildRowKey() {
		return Long.MAX_VALUE - System.currentTimeMillis();
	}
	
	

}
