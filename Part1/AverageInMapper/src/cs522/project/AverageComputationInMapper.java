package cs522.project;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageComputationInMapper {
	
	public static class MyMapper extends Mapper<LongWritable,Text,Text, TupleWritable> {
		
	    private final static IntWritable sum = new IntWritable(1);
	    private final static IntWritable count = new IntWritable(1);
	    private final static Text ip = new Text();
	    private final static TupleWritable tuple = new TupleWritable(new Writable[] {sum, count});
	    private Map<String, Pair> mapOutputs;
	    
	    
	    @Override
	    protected void setup(
	    		Mapper<LongWritable, Text, Text, TupleWritable>.Context context)
	    		throws IOException, InterruptedException {
	    	
	    	mapOutputs = new HashMap<>();
	    	super.setup(context);
	    	
	    }
	    
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, TupleWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] tokens = line.split(" ");
			if (tokens.length > 1) {
				try {
					String word = tokens[0];
					int quantity = Integer.parseInt(tokens[tokens.length - 1]);
					if (!mapOutputs.containsKey(word)) {
						mapOutputs.put(word, new Pair(quantity, 1));
					} else {
						Pair p = mapOutputs.get(word);
						p.addSum(quantity);
						p.addCount(1);						
					}
				} catch (NumberFormatException ex) {
					System.out.println("Error to process line: " + line);
				}
			}
		}
		
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, TupleWritable>.Context context)
				throws IOException, InterruptedException {
			
			for (Entry<String, Pair> entry : mapOutputs.entrySet()) {
				ip.set(entry.getKey());
				sum.set(entry.getValue().getSum());
				count.set(entry.getValue().getCount());
				context.write(ip, tuple);
			}
		}
		
	}
	
	public static class Reduce extends Reducer<Text, TupleWritable, Text, DoubleWritable> {
		private final static DoubleWritable average = new DoubleWritable(0);
		
		@Override
		protected void reduce(Text key, Iterable<TupleWritable> values,
				Reducer<Text, TupleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			int count = 0;
			for (TupleWritable t : values) {
				sum += ((IntWritable)t.get(0)).get();
				count += ((IntWritable)t.get(1)).get();
			}
			average.set(sum / count);
			context.write(key, average);
		}
	}

	public static void main(String[] args) throws Exception {
		
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AverageComputationInMapperCombine");
	        
	    job.setMapperClass(MyMapper.class);
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
	    job.setJarByClass(AverageComputationInMapper.class);
	    job.waitForCompletion(true);		

	}
}
