package cs522.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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


public class RelativeFrequencyHyBridInMapper {

	public static class InMapperCombiner extends Mapper<LongWritable, Text, Pair, IntWritable> {
	    private Map<Pair, IntWritable> mapOutput = null;
	        
	    @Override
	    protected void setup(
	    		Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
	    		throws IOException, InterruptedException {
	    	mapOutput = new HashMap<>();
	    	super.setup(context);
	    }
	    
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
	        		Pair p = new Pair(w, u);
	        		if (!mapOutput.containsKey(p)) {
	        			mapOutput.put(p, new IntWritable(1));
	        		} else {
	        			IntWritable c = mapOutput.get(p);
	        			c.set(c.get() + 1);
	        		}	        		
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
	    
	    @Override
	    protected void cleanup(
	    		Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
	    		throws IOException, InterruptedException {
	    	for (Entry<Pair, IntWritable> entry : mapOutput.entrySet()) {
	    		context.write(entry.getKey(), entry.getValue());
			}
	    }
	 }
	        
	 public static class Reduce extends Reducer<Pair, IntWritable, Text, Stripe> {
		private String m_prevTerm = null;
		private Stripe m_stripe;
		private double m_total = 0;
		
		@Override
		protected void setup(
				Reducer<Pair, IntWritable, Text, Stripe>.Context context)
				throws IOException, InterruptedException {
			m_prevTerm = null;
			m_total = 0;
			m_stripe = new Stripe();
			super.setup(context);
			
		}

		@Override
	    public void reduce(Pair key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        double sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        if (m_prevTerm != null && !m_prevTerm.equals(key.getFirst())) {
	        	m_stripe.divide(m_total);
	        	context.write(new Text(m_prevTerm), m_stripe);
	        	m_stripe.clear();
	        	m_total = 0;
	        }
	        m_prevTerm = key.getFirst();
	        m_stripe.put(new Text(key.getSecond()), new DoubleWritable(sum));
	        m_total += sum;
	    }
		
		@Override
		protected void cleanup(
				Reducer<Pair, IntWritable, Text, Stripe>.Context context)
				throws IOException, InterruptedException {
			m_stripe.divide(m_total);
        	context.write(new Text(m_prevTerm), m_stripe);
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
	        
	    Job job = Job.getInstance(conf, "RelativeFrequencyPairStripe");
	    
	    job.setOutputKeyClass(Pair.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(InMapperCombiner.class);
	    job.setReducerClass(Reduce.class);
	    job.setPartitionerClass(Partition.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setNumReduceTasks(1);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    Path outputDir = new Path(args[1]);
	    FileOutputFormat.setOutputPath(job, outputDir);
	    FileSystem fs = FileSystem.get(conf);
	    if (fs.exists(outputDir)) {
	        System.out.println("Delete output directory: " + outputDir);
	    	fs.delete(outputDir, true);
	    }
	    job.setJarByClass(RelativeFrequencyHyBridInMapper.class);
	    job.waitForCompletion(true);
	 }

}
