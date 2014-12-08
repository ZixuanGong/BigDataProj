package plan_three;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

public class VectorMapper extends Mapper<LongWritable,Text,Text,VectorWritable> {
	private static final String AGE="age";
	private static final String EDU="edu";
	private static final String INCOME="income";
	
	private Pattern splitter;
	private VectorWritable writer;
	private static Map<String,Integer> dictionary;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = splitter.split(value.toString());
		
		String cu = fields[0];
		String tag = fields[2] + "/"+ fields[1];
		NamedVector vector = new NamedVector(new SequentialAccessSparseVector(dictionary.size()), cu);
		vector.set(dictionary.get(tag), 1);
		writer.set(vector);
		context.write(new Text(cu), writer);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		
		for (String s: dictionary.keySet()) {
			System.out.println(s);
		}
		
		splitter = Pattern.compile(",");
		writer = new VectorWritable();
	}
	
	public static void setDictionary(Map<String,Integer> map) {
		dictionary = new HashMap<String, Integer>();
		dictionary = map;
		
		System.out.println("set dict!!");
	}
	
	
}

