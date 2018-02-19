package cs522.project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageComputation {
	
	public static class Map extends Mapper<LongWritable,Text,Text, IntWritable> {
		
	    private final static IntWritable lastQuantity = new IntWritable(1);
	    private Text ip = new Text();
	    
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] tokens = line.split(" ");
			if (tokens.length > 1) {
				ip.set(tokens[0]);
				try {
					lastQuantity.set(Integer.parseInt(tokens[tokens.length - 1]));
					context.write(ip, lastQuantity);
				} catch (NumberFormatException ex) {
					System.out.println("Error to process line: " + line);
				}
			}
		}
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		private final static DoubleWritable average = new DoubleWritable(0);
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			int count = 0;
			for (IntWritable c : values) {
				sum += c.get();
				count ++;
			}
			average.set(sum / count);
			context.write(key, average);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AverageComputation");
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    
	    Path outputDir = new Path(args[1]);
	    FileOutputFormat.setOutputPath(job, outputDir);
	    FileSystem fs = FileSystem.get(conf);
	    if (fs.exists(outputDir)) {
	        System.out.println("Delete output directory: " + outputDir);
	    	fs.delete(outputDir, true);
	    }
	    job.setJarByClass(AverageComputation.class);
	    job.waitForCompletion(true);		

	}
}
