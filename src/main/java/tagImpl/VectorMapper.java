package tagImpl;

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
	private Map<String,Integer> dictionary;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = splitter.split(value.toString());
		
		String cu = fields[0];
		String tag = fields[2] + "/"+ fields[1];
		double weight = 1;
		NamedVector vector = new NamedVector(new SequentialAccessSparseVector(dictionary.size() + 3), cu);
		vector.set(dictionary.get(tag), weight);
		writer.set(vector);
		context.write(new Text(cu), writer);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		generateDict(conf);
		
		splitter = Pattern.compile(",");
		writer = new VectorWritable();
	}
	
	private void generateDict(Configuration conf) throws IOException {
		dictionary = new HashMap<String,Integer>();
		Path dictionaryPath = new Path("data/dict");
		FileSystem fs = FileSystem.get(dictionaryPath.toUri(), conf); 
		FileStatus[] outputFiles = fs.globStatus(new Path(dictionaryPath, "part-*"));
		int i = 3;
		for (FileStatus fileStatus : outputFiles) {
			Path path = fileStatus.getPath();
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			Text key = new Text();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				dictionary.put(key.toString(), Integer.valueOf(i++));
			}
		}
		dictionary.put(AGE, 0);
		dictionary.put(EDU, 1);
		dictionary.put(INCOME, 2);
		for (String s: dictionary.keySet()){
			System.out.println(s + " "+dictionary.get(s));
		}

	}
}

