package com.hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class App3 {
	public static final String HDFS_PATH="hdfs://192.168.1.98:9000";
	public static final String DIR_PATH="/d1000";
	public static final String FILE_PATH="/d1000/f1000";
	
	/**
	 * @param args
	 * @throws URISyntaxException 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception, URISyntaxException {
		// TODO Auto-generated method stub
		downloadData();
		
		deleteData();
	}

	private static void deleteData() throws IOException, URISyntaxException {
		final FileSystem fileSystem=FileSystem.get(new URI(HDFS_PATH), new Configuration());
		fileSystem.delete(new Path(FILE_PATH), true);
	}

	private static void downloadData() throws IOException, URISyntaxException {
		final FileSystem fileSystem=FileSystem.get(new URI(HDFS_PATH), new Configuration());
		final FSDataInputStream in =fileSystem.open(new Path(FILE_PATH));
		IOUtils.copyBytes(in, System.out,1024,true);
	}

}
