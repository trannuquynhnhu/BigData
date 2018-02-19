package cs522.project;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeReducer extends Reducer<Text, Stripe, Text, Stripe> {
	
	@Override
	protected void reduce(Text key, Iterable<Stripe> values,
			Reducer<Text, Stripe, Text, Stripe>.Context context)
			throws IOException, InterruptedException {
		
		Stripe stripe = new Stripe();		
		for(Stripe value: values){
			stripe.addAll(value);
		}		
		stripe.divide(stripe.getTotalValue());
		context.write(key, stripe);		
	}

}
