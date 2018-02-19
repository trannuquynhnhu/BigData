package cs522.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripeMapper extends Mapper<LongWritable, Text, Text, Stripe> {

	Map<String, Stripe> globalHash = null;

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, Stripe>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		globalHash = new HashMap<String, Stripe>();
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Stripe>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		List<String> words = getWords(line);
		int length = words.size();
		for (int i = 0; i < length; i++) {
			String word = words.get(i);
			if (!globalHash.containsKey(word)) {
				globalHash.put(word, new Stripe());
			}
			Stripe stripe = globalHash.get(word);
			for (int j = i + 1; j < length; j++) {
				String neighbor = words.get(j);
				if (word.equals(neighbor)) {
					break;
				}
				Text u = new Text(neighbor);
				stripe.add(u, new IntWritable(1));
			}
		}

	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, Stripe>.Context context)
			throws IOException, InterruptedException {
		for (String key : globalHash.keySet()) {
			context.write(new Text(key), (Stripe) globalHash.get(key));
		}

		super.cleanup(context);
	}

	private List<String> getWords(String input) {
		List<String> output = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(input);

		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			output.add(token);
		}

		return output;
	}

}
