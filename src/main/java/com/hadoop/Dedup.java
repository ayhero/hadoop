package com.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Dedup {

	public static class Map extends Mapper<Object,Text,Text,Text>{
		
		public static Text line=new Text();//每行数据
		
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			line=value;
			context.write(line, new Text(""));
		}
		
		
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			context.write(key, new Text(""));
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			//数据准备
			String HDFS_PATH="hdfs://192.168.1.98:9000";
			/*
			FileSystem fileSystem=FileSystem.get(new URI(HDFS_PATH), new Configuration());
			if(!fileSystem.isDirectory(new Path("/inputs"))){
				fileSystem.mkdirs(new Path("/inputs"));
			}
			if(fileSystem.isDirectory(new Path("/outputs"))){
				fileSystem.delete(new Path("/outputs"));
			}
			if(!fileSystem.isFile(new Path("/inputs/file1.txt"))){
				FileInputStream in=new FileInputStream("D:/file1.txt");
				fileSystem.copyFromLocalFile(new Path("d:/file1.txt"), new Path("/inputs/file1.txt"));
			}
			if(!fileSystem.isFile(new Path("/inputs/file2.txt"))){
				FileInputStream in=new FileInputStream("D:/file2.txt");
				fileSystem.copyFromLocalFile(new Path("d:/file2.txt"), new Path("/inputs/file2.txt"));
			}*/
			//mapreduce计算			
			Configuration conf=new Configuration();
			//conf.set("fs.default.name", HDFS_PATH);
			//conf.set("hadoop.job.user", "hadoop");
			//conf.set("mapred.job.tracker", "192.168.1.98:9001");
			//JobConf conf=new JobConf();
			System.out.println("模式:"+conf.get("mapred.job.tracker"));
			Job job=new Job(conf,"Data Deduplication");
			job.setJarByClass(Dedup.class);
			//设置map,combine和reduce处理类
			job.setMapperClass(Map.class);
			job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);
			
			
			//设置输出类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);			
			
			Path inpath=new Path(HDFS_PATH+"/inputs");
			Path outpath=new Path(HDFS_PATH+"/outputs");
			
			FileInputFormat.addInputPath(job, inpath);
			FileOutputFormat.setOutputPath(job, outpath);
			
			System.exit(job.waitForCompletion(true)?0:1);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
