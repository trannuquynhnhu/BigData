package cs522.project;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {	
	
	private Text word = new Text();
	private Text key = new Text();
	private IntWritable value = new IntWritable();
    private HashMap<String, Integer> globalHash = null;      
        
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
    		throws IOException, InterruptedException {    	
    	super.setup(context);
    	globalHash = new HashMap<String, Integer>();
    }
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            String k = word.toString();
            
            if(globalHash.containsKey(k)) {            	
            	Integer newVal = globalHash.get(k) + 1;            	
            	globalHash.put(k, newVal);            
            }
            else {
            	globalHash.put(k, 1);
            }            
        }
    }    
    
    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) 
    		throws IOException, InterruptedException {    	
    	for(Entry<String, Integer> entry: globalHash.entrySet()) { 
    		key.set(entry.getKey());
    		value.set(entry.getValue());
    		context.write(key, value);    		    		
    	}    		
    	super.cleanup(context);
    	
    }
 } 