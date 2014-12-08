package tagImpl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.VectorWritable;

public class Main {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		int k = 10;
		
		runDictMapred(conf, new Path("assets/id_car.csv"));
		runVectorMapred(conf, new Path("assets/id_car.csv"));
	
		createInitClusterCenters(conf, k);
		
		
		KMeansDriver.run(conf, new Path("data/points"), new Path("data/clusters"), new Path("data/output"), 0.001, k, true, 0.1, false);
		
		printCluster(conf, new Path("data/output/clusters-10-final/part-r-00000"));
		
	}

	private static void printCluster(Configuration conf, Path path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		
		IntWritable key_output = new IntWritable();
		// Read output values
		ClusterWritable value_output = new ClusterWritable();
		
		while (reader.next(key_output, value_output)) {
		//	NamedVector vec = (NamedVector) value_output;
		//	double[] tmp = new double[3];
		//	tmp[0] = Math.round(vec.get(0) * 10);
		//	tmp[1] = Math.round(vec.get(1));
		//	tmp[2] = Math.round(vec.get(2) * 10000);
			
		//	Vector vec2 = new DenseVector(tmp);
			System.out.println(value_output.getValue().toString() + " / " + key_output.toString());
		}
		reader.close();
	}

	private static void createInitClusterCenters(Configuration conf, int k) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path("data/clusters/part-00000"), Text.class, Kluster.class);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("data/points/part-r-00000"), conf);
		
		Text key = new Text();
		VectorWritable value = new VectorWritable();
		
		for (int i = 0; i < k; i++) {
			reader.next(key, value);

			Kluster cluster = new Kluster(value.get(), i, new EuclideanDistanceMeasure());
			System.out.println(cluster.toString());
			writer.append(new Text(cluster.getIdentifier()), cluster);
		}
		reader.close();
		writer.close();
	}

	private static void runVectorMapred(Configuration conf, Path inputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "points");
		job.setMapperClass(VectorMapper.class);
		job.setReducerClass(VectorReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);
		SequenceFileOutputFormat.setOutputPath(job, new Path("data/points"));
		job.waitForCompletion(true);
	}

	private static void runDictMapred(Configuration conf, Path inputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "dictionary");
		job.setMapperClass(DictionaryMapper.class);
		job.setReducerClass(DictionaryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);
		SequenceFileOutputFormat.setOutputPath(job, new Path("data/dict"));
		job.waitForCompletion(true);
	}
}
