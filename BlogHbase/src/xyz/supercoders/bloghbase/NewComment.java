package xyz.supercoders.bloghbase;

import static xyz.supercoders.bloghbase.Constants.*;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class NewComment {

	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		
		System.out.println("Enter Row Key : ");
		long rowKey = sc.nextLong();
		sc.nextLine();
		System.out.println("Enter Comment : ");
		String comment = sc.nextLine();
		
		sc.close();
		
		try {
			insertComment(rowKey,comment);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	private static void insertComment(long rowKey, String comment) throws IOException {
		Configuration configuration = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf(TABLE_POSTS));
		
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(CF_COMMENTS, Bytes.toBytes(buildCommentKey()), Bytes.toBytes(comment));
		table.put(put);
		table.close();
		connection.close();
	}
	
	private static long buildCommentKey() {
		return Long.MAX_VALUE - System.currentTimeMillis();
	}

}
