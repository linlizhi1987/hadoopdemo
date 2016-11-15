package com.my.my_plugin_helloworld;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
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


public class Stay2Days {

	public static class LoginLogDayMapper extends
	        Mapper<LongWritable, Text, Text, Text> {
	
	    @Override
	    protected void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {

	    	StringTokenizer itr = new StringTokenizer(value.toString());
		    if(itr.hasMoreTokens())		
		    {
    			context.write(new Text(itr.nextToken()),new Text(""));
		    }
	    }
	}

	public static class LoginLogDayReducer extends
	        Reducer<Text, Text, Text, Text> {
	    
	    @Override
	    public void reduce(Text key, Iterable<Text> values,
	            Context context) throws IOException, InterruptedException {
	    	int count=0;
	    	for(Text v:values)
	    	{
	    		count=count+ 1;
	    	}
	    	if(count==2)
	    	{
	    		context.write(key, new Text(""));
	    	}
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
	    Configuration conf = new Configuration();
	
	    //args=new String[]{"hdfs://172.17.155.32:8900/llz/Stay2Days/DayDistinct/20150108.txt,hdfs://172.17.155.32:8900/llz/Stay2Days/DayNew/20150107.txt","/llz/output/"};
	    
	    Job job = Job.getInstance(conf, "Stay2Days");
	    job.setJarByClass(Stay2Days.class);
	    job.setMapperClass(LoginLogDayMapper.class);
	    job.setReducerClass(LoginLogDayReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    

	    String[] InputPaths = args[0].split(",");
	    for(String InputPath : InputPaths)
	    {	    	
	    	FileInputFormat.addInputPath(job, new Path(InputPath));
	    }
	    FileSystem.get(conf).delete(new Path(args[1]), true);
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	    

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
