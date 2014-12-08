package plan_three;

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
	    String tag = fields[2] + "/" + fields[1];
	    context.write(new Text(tag), new IntWritable(0));
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    super.setup(context);
	    splitter = Pattern.compile(",");
	}
}
