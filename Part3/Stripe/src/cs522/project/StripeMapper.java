package cs522.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StripeMapper extends Mapper<LongWritable, Text, Text, Stripe> {	
		
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Stripe>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();			
		List<String> words = getWords(line);
		String word = "";		
		int length = words.size();
		for(int i = 0; i < length; i++){
			word = words.get(i);			
			Stripe stripe = new Stripe();
			
			for(int j = i + 1; j < length; j++) {	
				String neighbor = words.get(j);
				if (word.equals(neighbor)) {
					break;
				}			
				stripe.add(new Text(neighbor), new IntWritable(1));
			}
			context.write(new Text(word), stripe);
		}	
		
	}
	
	private List<String> getWords(String input){
		List<String> output = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(input);
		
		while(tokenizer.hasMoreTokens()){
			String token = tokenizer.nextToken(); 
			output.add(token);
		}
		
		return output;		
	}
	

}
