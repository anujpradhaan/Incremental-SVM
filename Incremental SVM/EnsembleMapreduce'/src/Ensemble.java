import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Ensemble {

	
	
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	    private final static LongWritable one = new LongWritable();
	    private Text word = new Text();
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	      //  Integer.parseInt(word.getBytes().toString());
	        //System.out.println("Key is ="+key.toString()+"value is"+value.toString());
	       
	        StringTokenizer tokenizer = new StringTokenizer(line,"|");
	        
	        LongWritable l2= new LongWritable();
	        l2.set((long) Float.parseFloat(tokenizer.nextToken()));
	        //word.set(tokenizer.nextToken());
	       // long a=((long) Float.parseFloat(tokenizer.nextToken()));
	     //  System.out.println("a="+a);
	        
	        LongWritable l= new LongWritable(Long.parseLong(tokenizer.nextToken()));
            //tokenizer.nextToken();
	 	      //System.out.println("Key is ="+l.toString()+"value is"+word.toString());     
	 	      context.write(l, l2);
           // one.set(2);
           
          // System.exit(0);
            //int linecount=0;
	       
	        	
	       // 	 one.set((long) Float.parseFloat(tokenizer.nextToken()));
	 	     //   word.set(tokenizer.nextToken());
	 	      // System.out.println("Key is ="+word.toString()+"value is"+one.toString());     
	        	//context.write(one, word);
	        
	    }
	 } 
	        
	 public static class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        long sum = 0;
	        for (LongWritable val : values) {
	        	long a=val.get();
	        	sum += a;
	        }
	        
	       
	        context.write(key, new LongWritable(sum>0?1:-1));
	    }
	 }
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("mapred.textoutputformat.separator", " ");
	        Job job = new Job(conf, "wordcount");
	        FileSystem filesys;
	        filesys = FileSystem.get(URI.create("hdfs://master1:54310/user/hduser/"), conf);
	        
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
		job.setJarByClass(Ensemble.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(filesys.getWorkingDirectory()+"/ensemblefile"));
		FileOutputFormat.setOutputPath(job, new Path("op/11"));
	        
	    job.waitForCompletion(true);
	    
	    
	 
	    
	 }

	
	
	        
	}
	 
		  
		  
	  
	
	

