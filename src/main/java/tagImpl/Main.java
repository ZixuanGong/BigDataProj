package tagImpl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericsUtil;
import org.apache.mahout.math.VectorWritable;

public class Main {
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		Job job, job2;
		try {
//			job = new Job(conf, "dictionary");
//			job.setMapperClass(DictionaryMapper.class);
//			job.setReducerClass(DictionaryReducer.class);
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(IntWritable.class);
//			job.setOutputFormatClass(SequenceFileOutputFormat.class);
//			FileInputFormat.addInputPath(job, new Path("assets/id_car.csv"));
//			SequenceFileOutputFormat.setOutputPath(job, new Path("output"));
//			job.waitForCompletion(true);
//			
////			generateDict(conf);
//			
//			job2 = new Job(conf, "tag_vectors");
//			job2.setMapperClass(VectorMapper.class);
//			job2.setReducerClass(VectorReducer.class);
//			job2.setOutputKeyClass(Text.class);
//			job2.setOutputValueClass(VectorWritable.class);
//			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
//			FileInputFormat.addInputPath(job2, new Path("assets/id_car.csv"));
//			SequenceFileOutputFormat.setOutputPath(job2, new Path("tag_vectors"));
//			job2.waitForCompletion(true);
			
			int k = 30;

			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs,
	                  new Path("tag_vectors/part-r-00000"), conf);

			Text key = new Text();
			VectorWritable value = new VectorWritable();
			for (int i = 0; i < k; i++) {
				reader.next(key, value);
				System.out.println(value.toString());

			}
			
		} catch (IOException e) {
			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
		}

		

		// Path path = new Path("clusters/part-m-00000");
		// for (int i = 0; i < k; i++) {
		// 	Vector vec = (Vector) points.get(i);

		// 	// write the initial center here as vec
		// 	Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
		// 	writer.append(new Text(cluster.getIdentifier()), cluster);
		// 	}

		// 	writer.close();
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

//	private static void generateDict(Configuration conf) throws IOException {
//		Map<String,Integer> dictionary = new HashMap<String,Integer>();
//		Path dictionaryPath = new Path("output");
//		FileSystem fs = FileSystem.get(dictionaryPath.toUri(), conf); 
//		FileStatus[] outputFiles = fs.globStatus(new Path(dictionaryPath, "part-*"));
//		int i = 0;
//		for (FileStatus fileStatus : outputFiles) {
//			Path path = fileStatus.getPath();
//			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
//			Text key = new Text();
//			IntWritable value = new IntWritable();
//			while (reader.next(key, value)) {
//				dictionary.put(key.toString(), Integer.valueOf(i++));
//				System.out.println(key.toString() + " " + dictionary.get(key.toString()));
//			} 
//		}
//
//	}

}
