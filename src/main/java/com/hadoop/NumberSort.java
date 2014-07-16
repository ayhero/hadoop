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

import com.Resources;
import com.Utils;

public class NumberSort {

	public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
		
		public static IntWritable data=new IntWritable();//每行数据
		
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			String line=value.toString();
			data.set(Integer.valueOf(line));
			context.write(data, new IntWritable(1));
		}
		
		
	}
	
	public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		
		public static IntWritable index=new IntWritable(1);
		
		public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			
			context.write(index, key);
			index=new IntWritable(index.get()+1);
			
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			String indir="file/numberSort";
			String outdir=indir+"/outputfiles";
			//Utils.deleteDir(outdir);//删除输出目录
			//mapreduce计算			
			Configuration conf=new Configuration();
			System.out.println("模式:"+conf.get("mapred.job.tracker"));
			Job job=new Job(conf,"NumberSort");
			job.setJarByClass(NumberSort.class);
			//设置map,combine和reduce处理类
			job.setMapperClass(Map.class);
			//job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);
			
			//设置输出类型
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);			
			//设置输出路径
			FileInputFormat.addInputPath(job, new Path(indir));
			FileOutputFormat.setOutputPath(job, new Path(outdir));
			
			System.exit(job.waitForCompletion(true)?0:1);
			
			
		} catch (IOException e) {
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
