package com.my.my_plugin_helloworld;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import com.opencsv.CSVParser;

public class Stay2Days_Dis {

	public static class LoginLogDayMapper extends
	        Mapper<LongWritable, Text, Text, IntWritable> {
	
	    @Override
	    protected void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {
	
	    	if(key.get()>0)
	    	{
	    		try{
	    			CSVParser parser = new CSVParser();
	    			String[] lines=parser.parseLine(value.toString());
	    			
	    			/*
	    			SimpleDateFormat formatter= new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	    			Date dt=formatter.parse(lines[3]);
	    			formatter.applyPattern("yyyy-MM-dd");
	    			String dtstr=formatter.format(dt);*/
	    			context.write(new Text(lines[0]),new IntWritable(1));
	    		}
	    		catch(Exception e){}
	    		
	    	}
	    }
	}

	public static class LoginLogDayReducer extends
	        Reducer<Text, IntWritable, Text, IntWritable> {
	    
	    @Override
	    public void reduce(Text key, Iterable<IntWritable> values,
	            Context context) throws IOException, InterruptedException {
	    	int count=0;
	    	for(IntWritable value:values)
	    	{
	    		count=count+ value.get();
	    	}
	        context.write(key, new IntWritable(count));
	    }
	}
	
	public static class RegexIncludePathFilter implements PathFilter {
	     private final String regex;
	 
	     public RegexIncludePathFilter(String regex) {
	         this.regex = regex;
	     }
	 
	     public boolean accept(Path path) {
	         return path.toString().matches(regex);
	    }
	}

	public static void main(String[] args) throws Exception {
	
	    //args=new String[]{"hdfs://172.17.155.32:8900/llz/input","hdfs://172.17.155.32:8900/llz/output","2015010701"};

	    Job job = Job.getInstance();
	    
	    Configuration conf = job.getConfiguration();
	    String tmpjars = conf.get("tmpjars");
	    String appendJars = "/llz/soft/opencsv-3.1.jar";
	    if (tmpjars == null) {
	        conf.set("tmpjars", appendJars);
	    } else {
	        conf.set("tmpjars", tmpjars+","+appendJars);
	    }
	     
	    
	    job.setJarByClass(Stay2Days_Dis.class);
	    job.setMapperClass(LoginLogDayMapper.class);
	    job.setCombinerClass(LoginLogDayReducer.class);
	    job.setReducerClass(LoginLogDayReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	   /* FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
	    if(args.length>2)
	    {
		    FileStatus[] status = fs.globStatus(new Path(args[0]+"/"+args[2])) ;
		    Path[] listedPaths = FileUtil.stat2Paths(status);
		    for(Path ph : listedPaths)
		    {	    	
		    	FileInputFormat.addInputPath(job, ph);
		    }
	    }
	    else*/
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileSystem.get(conf).delete(new Path(args[1]),true);
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
    

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
