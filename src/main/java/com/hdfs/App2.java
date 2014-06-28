package com.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class App2 {
	
	public static final String HDFS_PATH="hdfs://192.168.1.98:9000";
	public static final String DIR_PATH="/d1000";
	public static final String FILE_PATH="/d1000/f1000";
	/**
	 * @param args
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
	  createDirandFile();
	}
	private static void createDirandFile() throws IOException,
			URISyntaxException, FileNotFoundException {
		final FileSystem fileSystem=FileSystem.get(new URI(HDFS_PATH), new Configuration());
		  fileSystem.mkdirs(new Path(DIR_PATH));
		  final FSDataOutputStream out=fileSystem.create(new Path(FILE_PATH));
		  final FileInputStream in=new FileInputStream("f:/hello.log");
		  IOUtils.copyBytes(in, out,1024,true);
	}
	

}
