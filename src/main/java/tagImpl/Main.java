package tagImpl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class Main {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job, job2;
		int k = 10;
		try {
			job = new Job(conf, "dictionary");
			job.setMapperClass(DictionaryMapper.class);
			job.setReducerClass(DictionaryReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path("assets/id_car.csv"));
			SequenceFileOutputFormat.setOutputPath(job, new Path("data/output"));
			job.waitForCompletion(true);
			
			job2 = new Job(conf, "points");
			job2.setMapperClass(VectorMapper.class);
			job2.setReducerClass(VectorReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(VectorWritable.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(job2, new Path("assets/id_car.csv"));
			SequenceFileOutputFormat.setOutputPath(job2, new Path("data/points"));
			job2.waitForCompletion(true);

			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		FileSystem fs;
		
		fs = FileSystem.get(conf);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path("data/clusters/part-00000"), 
				Text.class, Kluster.class);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs,
               new Path("data/points/part-r-00000"), conf);
		
		
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
		
		
		KMeansDriver.run(conf, new Path("data/points"), new Path("data/clusters"),
		        new Path("data/output"), 0.001, k, true, 0.1, false);
		reader = new SequenceFile.Reader(fs,
		                  new Path("data/output/clusteredPoints/part-m-00000"), conf);
		IntWritable key_output = new IntWritable();
		// Read output values
		WeightedPropertyVectorWritable value_output = new WeightedPropertyVectorWritable();
       
       while (reader.next(key_output, value_output)) {
//        	NamedVector vec = (NamedVector) value_output;
//        	double[] tmp = new double[3];
//        	tmp[0] = Math.round(vec.get(0) * 10);
//			tmp[1] = Math.round(vec.get(1));
//			tmp[2] = Math.round(vec.get(2) * 10000);
       	
//        	Vector vec2 = new DenseVector(tmp);
           System.out.println(value_output.toString() + " belongs to cluster " + key_output.toString());
       }
       reader.close();
	}
}

}
