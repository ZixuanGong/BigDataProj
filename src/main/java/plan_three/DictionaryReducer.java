package plan_three;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DictionaryReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	@Override
	protected void reduce(Text tag, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		context.write(tag, new IntWritable(0));
	} 
}
