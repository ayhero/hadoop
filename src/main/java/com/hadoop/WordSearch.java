package com.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.Local_Utils;
import com.Resources;
import com.HDFS_Utils;

public class WordSearch {

	public static int num=0;
	public static class Maps extends Mapper<LongWritable,Text,Text,Text>{
		
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			StringTokenizer st=new StringTokenizer(value.toString());
			while(st.hasMoreTokens()){
				FileSplit split=(FileSplit) context.getInputSplit();
				String path=split.getPath().getName();
				context.write(new Text(st.nextToken()),new Text(path));
			}
			
		}
		
		
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			Map<String,Integer> map=new HashMap<String,Integer>();
			Iterator<Text> iter=values.iterator();
			while(iter.hasNext()){
				String tmp=iter.next().toString();
				if(map.containsKey(tmp)){
					map.put(tmp, map.get(tmp)+1);
				}else{
					map.put(tmp, 1);
				}
			}
			String output="";
			for(Entry<String, Integer> e : map.entrySet()){
				output=output+e.getKey()+":"+e.getValue()+";";
			}
			context.write(key, new Text(output));
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			String file="wordSearch";
			String indir="file/"+file;
			String outdir=indir+"/outputfiles";
			Local_Utils.deleteLocalDir(outdir);//删除输出目录
			//mapreduce计算			
			Configuration conf=new Configuration();
			System.out.println("模式:"+conf.get("mapred.job.tracker"));
			Job job=new Job(conf,file);
			job.setJarByClass(WordSearch.class);
			//设置map,combine和reduce处理类
			job.setMapperClass(Maps.class);
			//job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);
			
			//设置输出类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);			
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
