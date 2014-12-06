package tagImpl;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DictionaryMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private Pattern splitter;

	@Override
	protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
	    String[] fields = splitter.split(line.toString());
//	    if (fields.length < 4) {
//			context.getCounter("Map", "LinesWithErrors").increment(1);
//			return; 
//		}
		String cu = fields[0];
		System.out.println("cu = " + cu);
		context.write(new Text(cu), new IntWritable(0));
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    super.setup(context);
	    splitter = Pattern.compile(",");
	}
}
