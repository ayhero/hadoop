package com.hdfs;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class App1 {
	
	public static final String HDFS_PATH="hdfs://192.168.1.98:9000/hello.txt";
	
	public static void main(String[] args) throws Exception{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		final URL url=new URL(HDFS_PATH);
		final InputStream in =url.openStream();
		IOUtils.copyBytes(in, System.out,1024,true);
	}
	
	
}
