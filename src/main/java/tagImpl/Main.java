package tagImpl;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Main {
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		Job job;
		try {
			job = new Job(conf, "dictionary");
			job.setMapperClass(DictionaryMapper.class);
			job.setReducerClass(DictionaryReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path("assets/id_car.csv"));
			SequenceFileOutputFormat.setOutputPath(job, new Path("output"));
			job.waitForCompletion(true);
			
			generateDict(conf);
			
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
//		FileSystem fs;
//		try {
//			fs = FileSystem.get(conf);
//			SequenceFile.Reader reader = new SequenceFile.Reader(fs,
//	                new Path("output/part-r-00000"), conf);
//			
//			Text key = new Text();
//			IntWritable value = new IntWritable();
//			
//			while(reader.next(key, value)) {
//				System.out.println(key + value.toString());
//			}
//			reader.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
	}

	private static void generateDict(Configuration conf) throws IOException {
		ArrayList<String> dictionary = new ArrayList<String>();
		Path dictionaryPath = new Path("output");
		FileSystem fs = FileSystem.get(dictionaryPath.toUri(), conf); 
		FileStatus[] outputFiles = fs.globStatus(new Path(dictionaryPath, "part-*"));
		int i = 0;
		for (FileStatus fileStatus : outputFiles) {
			Path path = fileStatus.getPath();
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			Text key = new Text();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				dictionary.add(key.toString());
				System.out.println(key.toString());
			} 
		}
		
		

	}
	
	

}
