package cs522.project;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class Stripe extends MapWritable {
	
	public void addAll(Stripe from) {
        for (Writable fromKey : from.keySet()) {
            add(fromKey, from.get(fromKey));
        }
    }
	
	/*
	 * @param value: is either IntWritable or DoubleWritable
	 */
	public void add(Writable key, Writable value) {		
		if (containsKey(key)) {
			if (value instanceof DoubleWritable) {
				DoubleWritable currentValue = (DoubleWritable) get(key);
				currentValue.set(currentValue.get() + ((DoubleWritable)value).get());     
			} else {
				IntWritable currentValue = (IntWritable) get(key);
				currentValue.set(currentValue.get() + ((IntWritable)value).get()); 
			}
        } else {
            put(key, value);
        }
	}
	
	public double getTotalValue(){
		double total = 0;
		for(Writable value: values()){
			if (value instanceof IntWritable) {
				total += ((IntWritable) value).get();
			} else {
				total += ((DoubleWritable) value).get();
			}
		}
		return total;
	}
	
	public void divide(double number) {
		for (Writable key : keySet()) {
			Writable value = get(key);
			if (value instanceof DoubleWritable) {
				DoubleWritable d = (DoubleWritable) value;
				d.set(d.get() / number);
			} else {
				double d = ((IntWritable) value).get();
				put(key, new DoubleWritable(d / number));
			}
		}
	}

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder("{");
        for (Writable key : keySet()) {
            buffer.append(key).append(":").append(get(key)).append(",");
        }
        return buffer.substring(0, buffer.length() - 1) + "}";
    }

}
