package xyz.supercoders.bloghbase;

import org.apache.hadoop.hbase.util.Bytes;

public class Constants {

	public static final String TABLE_POSTS = "posts";
	
	public static final byte[] CF_INFO = Bytes.toBytes("info");
	public static final byte[] CF_DETAILS = Bytes.toBytes("details");
	public static final byte[] CF_COMMENTS = Bytes.toBytes("comments");
	
	public static final byte[] C_TITLE = Bytes.toBytes("title");
	public static final byte[] C_AUTHOR = Bytes.toBytes("author");
	public static final byte[] C_DATE = Bytes.toBytes("date");
	public static final byte[] C_DESCRIPTION = Bytes.toBytes("description");
}
