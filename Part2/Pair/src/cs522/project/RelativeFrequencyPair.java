package cs522.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class RelativeFrequencyPair {

	public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Pair pair = new Pair("","");
	    private Pair wildcard = new Pair("", Pair.WILD_CARD);
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        List<String> words = getWords(value.toString());
	        int length = words.size();
	        for (int i = 0; i < length; i++) {
	        	String w = words.get(i);
	        	for (int j = i + 1; j < length; j++) {
	        		String u = words.get(j);
	        		if (u.equals(w)) {
	        			break;
	        		}	        		
	        		pair.setFirst(w);
	        		pair.setSecond(u);
	        		context.write(pair, one);
	        		wildcard.setFirst(w);
	        		context.write(wildcard, one);
	        	}
	        }
	    }
	    
	    private List<String> getWords(String line) {
	    	List<String> result = new ArrayList<>();
	    	StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
	        	result.add(tokenizer.nextToken());
	        }
	        return result;
	    }
	 } 
	        
	 public static class Reduce extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
		double m_total = 0;
		
		@Override
		protected void setup(
				Reducer<Pair, IntWritable, Pair, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			
		}

		@Override
	    public void reduce(Pair key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        double sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        if (!key.getSecond().equals(Pair.WILD_CARD)) {
	        	context.write(key, new DoubleWritable(sum / m_total));
	        } else {
	        	m_total = sum;
	        }
	    }
	 }
	        
	 public static class Partition extends Partitioner<Pair, IntWritable> {
		@Override
		public int getPartition(Pair key, IntWritable value, int numReducers) {
			
			return Math.abs(key.getFirst().hashCode()) % numReducers;
		}
		 
	 }
	 
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = Job.getInstance(conf, "RelativeFrequencyPairApproach");
	    
	    job.setOutputKeyClass(Pair.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setPartitionerClass(Partition.class);
	        
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
	    job.setJarByClass(RelativeFrequencyPair.class);
	    job.waitForCompletion(true);
	 }

}
