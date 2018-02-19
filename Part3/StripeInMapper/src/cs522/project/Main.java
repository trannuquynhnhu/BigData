package cs522.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Main {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();        
        Job job = Job.getInstance(conf, "relativeFrequencyStripe");    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Stripe.class);
	        
	    job.setMapperClass(StripeMapper.class);
	    job.setReducerClass(StripeReducer.class);
	        
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
	        
	    job.setJarByClass(Main.class);
	    job.waitForCompletion(true);

	}

}
