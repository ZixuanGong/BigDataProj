package tagImpl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

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
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class Clusterer {
	Configuration conf;
	HashMap<Long, String> eduMap;
	HashMap<String,Integer> descr2idx;
	HashMap<Integer, String> idx2descr;
	
	public Clusterer(Configuration configuration) throws IOException, ClassNotFoundException, InterruptedException {
		this.conf = configuration;
		eduMap = new HashMap<Long, String>();
		
		int k = 10;
		
		runDictMapred(new Path("assets/id_car.csv"));
		
		generateDict();
		VectorMapper.setDictionary(descr2idx);
		
		runVectorMapred(new Path("assets/id_car.csv"));
	
		createInitClusterCenters(k);
		
		
		KMeansDriver.run(conf, new Path("data/points"), new Path("data/clusters"), new Path("data/output"), 0.001, k, true, 0.1, false);
		
		importEduMap("assets/edu_code");
		
		printCluster(new Path("data/output/clusters-10-final/part-r-00000"));
		
	}


	private void importEduMap(String path) {
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(path));
			String line = "";

			while ((line = br.readLine()) != null) {
				String[] tokens = line.split(" ", 2);
				long code = Long.parseLong(tokens[0]);
				String descr = tokens[1];

				eduMap.put(code, descr);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		new Clusterer(new Configuration());
		
	}

	private void printCluster(Path path) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		
		IntWritable key = new IntWritable();
		ClusterWritable value = new ClusterWritable();
		
		while (reader.next(key, value)) {
			
			Vector center = value.getValue().getCenter();
			Vector radius = value.getValue().getRadius();
			long age_l = Math.round((center.get(0) - radius.get(0))*10);
			long age_h = Math.round((center.get(0) + radius.get(0))*10);
 			long edu_l = Math.round(center.get(1) - radius.get(1));
 			long edu_h = Math.round(center.get(1) + radius.get(1));
			long income_l = Math.round((center.get(2) - radius.get(2)) * 10000);
			long income_h = Math.round((center.get(2) + radius.get(2)) * 10000);
			if (income_l < 0)
				income_l = 0;
			
			HashMap<Integer, Double> idx_val = new HashMap<Integer, Double>();
			double val;
			for (int i = 3; i < center.size(); i++) {
				val = center.get(i);
				idx_val.put(i, val);
			}
			
	        ValueComparator bvc =  new ValueComparator(idx_val);
	        TreeMap<Integer,Double> sorted_map = new TreeMap<Integer,Double>(bvc);

	        sorted_map.putAll(idx_val);
	        String eduRangeString = eduMap.get(edu_l);
	        if (edu_l != edu_h) {
	        	eduRangeString += " ~ " + eduMap.get(edu_h);
	        }
	        System.out.print("Cluster " + key.get() + ":" + 
	        		"\n\t age = " + age_l + " ~ " + age_h +
	        		"\n\t edu = " + eduRangeString + 
	        		"\n\t income = " + income_l + " ~ " + income_h + 
	        		"\n\t top cars = \n");
	        
	        int i = 0;
	        for(int idx: sorted_map.keySet()) {
	        	if (i > 2)
	        		break;
	        	
	        	String descr = idx2descr.get(idx);
	        	System.out.println("\t\t" + i + " " + descr + "->" + idx_val.get(idx));
	        	i++;
	        }
	        System.out.print("\n");
//	        dbg(value.getValue().toString());
			
		}
		reader.close();
	}
	
	private void generateDict() throws IOException {
		descr2idx = new HashMap<String,Integer>();
		idx2descr = new HashMap<Integer, String>();
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
				descr2idx.put(key.toString(), Integer.valueOf(i++));
			}
		}
		descr2idx.put("age", 0);
		descr2idx.put("edu", 1);
		descr2idx.put("income", 2);
		for (String s: descr2idx.keySet()){
			idx2descr.put(descr2idx.get(s), s);
			System.out.println(s + " "+descr2idx.get(s));
		}
	}

	private void createInitClusterCenters(int k) throws IOException {
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

	private void runVectorMapred(Path inputPath) throws IOException, ClassNotFoundException, InterruptedException {
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

	private void runDictMapred(Path inputPath) throws IOException, ClassNotFoundException, InterruptedException {
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
	
	private void dbg(String msg) {
		System.out.println(msg);
	}
}
