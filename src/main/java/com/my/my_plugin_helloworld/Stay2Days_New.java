package com.my.my_plugin_helloworld;

import java.io.BufferedReader;
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


public class Stay2Days_New {

	public static class LoginLogDayMapper extends
	        Mapper<LongWritable, Text, Text, Text> {
	
	    @Override
	    protected void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {
	
	    	StringTokenizer itr = new StringTokenizer(value.toString());
		    if(itr.hasMoreTokens())		
		    {
		    	String mapkey="";
    			String mapval="";
    			if(itr.countTokens()>1)
    			{
    				mapval="D";
    			}
    			else
    			{
    				mapval="N";
    			}
    			mapkey=itr.nextToken();
    			context.write(new Text(mapkey),new Text(mapval));
		    }
	    }
	}

	public static class LoginLogDayReducer extends
	        Reducer<Text, Text, Text, Text> {
	    
	    @Override
	    public void reduce(Text key, Iterable<Text> values,
	            Context context) throws IOException, InterruptedException {
	    	Boolean isN=false;
		    int count=0;
	    	for(Text v:values)
	    	{
	    		count=count+ 1;
	    		if(v.toString().equals("D"))
	    		{
	    			isN=true;
	    		}
	    	}    	
	    	if(count==1 && isN)
	    	{
	    		context.write(new Text(key.toString()), new Text(""));
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
	
	    //args=new String[]{"hdfs://172.17.155.32:8900/llz/input","hdfs://172.17.155.32:8900/llz/output","2015010701"};

	    Job job = Job.getInstance(conf, "Stay2Days_New");
	    job.setJarByClass(Stay2Days_New.class);
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
