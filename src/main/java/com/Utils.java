package com;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utils {
	public static FileSystem fileSystem=null;
	
	static {
		try {
			fileSystem=FileSystem.get(new URI(Resources.HDFS_PATH), new Configuration());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 创建HDFS路径
	 */
	public static void createDir(String url){
		try {
			
			if(!fileSystem.exists(new Path(url))){
					fileSystem.mkdirs(new Path(url));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 上传文件到hdfs
	 */
	public static void uploadFile(String src,String target){
		try{
			if(!fileSystem.isFile(new Path(target))){
				fileSystem.copyFromLocalFile(new Path(src), new Path(target));
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 删除hdfs文件、文件夹
	 */
	public static void deleteDir(String dir){
		
		try{
			if(fileSystem.exists(new Path(dir))){
				fileSystem.delete(new Path(dir));
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	/**
	 * 删除本地文件夹
	 * @param dir
	 */
	public static void deleteLocalDir(String dir){
		
	}
}
