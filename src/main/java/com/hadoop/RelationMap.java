package com.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.Local_Utils;
import com.Resources;
import com.HDFS_Utils;

public class RelationMap {

	public static int num=0;
	public static class Map extends Mapper<LongWritable,Text,Text,Text>{
		
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			String line=value.toString();
			StringTokenizer st=new StringTokenizer(value.toString());
			String[] relations=new String[2];
			int count=0;
			//对标题行忽略
			if(line.indexOf("child")!=-1){
				return;
			}
			System.out.println("读入一行:字符数"+st.countTokens());
			if(st.countTokens()<1){
				return;
			}
			while(st.hasMoreTokens()){
				if(count>=2){
					System.out.println(st.nextToken());
					break;
				}
				relations[count]=st.nextToken();
				count++;
			}
			//格式为:(John,2+Tom+John),2表示该关系中是父亲，1表示该关系中的孩子
			context.write(new Text(relations[0]),new Text("1+"+relations[0]+"+"+relations[1]));
			context.write(new Text(relations[1]),new Text("2+"+relations[0]+"+"+relations[1]));
			
		}
		
		
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			if(num==0){
				context.write(new Text("grandchild"),new Text("grandparent"));
				num++;
			}
			List<String> childs=new ArrayList<String>(),parents=new ArrayList<String>();
			Iterator<Text> iter=values.iterator();
			while(iter.hasNext()){
				String[] temp=iter.next().toString().split("\\+");
				if("1".equals(temp[0])){
					parents.add(temp[2]);//如果当前的key是作为子辈，就只抽取
				}else{
					childs.add(temp[1]);//如果当前的key是作为父辈，就只抽取子辈
				}
				
			}
			//如果只作为子辈或只作为父辈的，那么这些就是所有的孙子和所有的爷爷
			//仅仅抽出这些没有办法配对关系
			//grandchild!=0 并且 grandparent!=0表明这个关系中，key是作为爸爸一层的，这样才能看出孙子和爷爷的对应关系
			//只要是对同一个爸爸的，那么也就对同一个爷爷所以孙辈和爷爷辈的输出要用两个循环做
			if(childs.size()!=0 && childs.size()!=0){
				for(String child : childs){
					for(String parent : parents){
						context.write(new Text(child), new Text(parent));
					}
				}
			}
			
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			String file="relationMap";
			String indir="file/"+file;
			String outdir=indir+"/outputfiles";
			Local_Utils.deleteLocalDir(outdir);//删除输出目录
			//mapreduce计算			
			Configuration conf=new Configuration();
			System.out.println("模式:"+conf.get("mapred.job.tracker"));
			Job job=new Job(conf,file);
			job.setJarByClass(RelationMap.class);
			//设置map,combine和reduce处理类
			job.setMapperClass(Map.class);
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
