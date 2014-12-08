package plan_three;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class Clusterer {
	Configuration conf;
	HashMap<Long, String> eduMap;
	HashMap<Long, String> stateMap;
	HashMap<String,Integer> descr2idx;
	HashMap<Integer, String> idx2descr;
	HashMap<String, SequentialAccessSparseVector> consumerUnitInfo;
	int vec_size;
	int car_base;
	int state_base;
	
	public Clusterer(Configuration configuration) throws IOException, ClassNotFoundException, InterruptedException {
		this.conf = configuration;
		
		int k = 10;
		eduMap = importCodeDescrMap("assets/edu_code");
		stateMap = importCodeDescrMap("assets/state_code");
				
		runDictMapred(new Path("assets/id_car.csv"));
		generateDict();
		getCuInfo();
		VectorMapper.setDictionary(descr2idx);
		VectorReducer.setCuInfo(consumerUnitInfo);
		
		runVectorMapred(new Path("assets/id_car.csv"));
	
		createInitClusterCenters(k);
		KMeansDriver.run(conf, new Path("data/points"),
				new Path("data/clusters"),
				new Path("data/output"),
				0.001, k, true, 0.1, false);
		
		
		printClusters(new Path("data/output/clusters-10-final/part-r-00000"));
		printPoints(new Path("data/points/part-r-00000"));
		
	}
	
	private void getCuInfo() throws IOException {
		consumerUnitInfo = new HashMap<String, SequentialAccessSparseVector>();
		
		BufferedReader br = null;
    	String line = "";
     
		br = new BufferedReader(new FileReader("assets/id_info_valid.csv"));
		while ((line = br.readLine()) != null) {
 
			String[] tokens = line.split(",");
			String id = tokens[0];
			try {
				double age = Double.parseDouble(tokens[1]);
				double edu = Double.parseDouble(tokens[2]);
				double income = Double.parseDouble(tokens[3]);
				int state = Integer.parseInt(tokens[4]);
				
				SequentialAccessSparseVector vector = new SequentialAccessSparseVector(vec_size);
				vector.set(0, age/10);
				vector.set(1, edu);
				vector.set(2, income/10000);
				vector.set(find_col_by_code(stateMap, state), 1);
				consumerUnitInfo.put(id, vector);
				
			} catch (NumberFormatException e) {
				continue;
			}
		}
		br.close();
	}


	private int find_col_by_code(HashMap<Long, String> map, int code) {
		dbg("code=" + code);
		String descr = map.get(new Long(code));
		dbg("descr=" + descr);
		return descr2idx.get(descr);
	}

	private HashMap<Long, String> importCodeDescrMap(String path) throws NumberFormatException, IOException {
		HashMap<Long, String> map = new HashMap<Long, String>();
		
		BufferedReader br;
		br = new BufferedReader(new FileReader(path));
		String line = "";

		while ((line = br.readLine()) != null) {
			String[] tokens = line.split(" ", 2);
			long code = Long.parseLong(tokens[0]);
			String descr = tokens[1];

			map.put(code, descr);
		}
		
		br.close();
		return map;
	}

	private void printPoints(Path path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		
		Text key = new Text();
		VectorWritable value = new VectorWritable();
		
		while (reader.next(key, value)) {
			Vector vector = value.get();
			dbg(vector.toString());
		}
	}

	private void printClusters(Path path) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		
		IntWritable cluster_id = new IntWritable();
		ClusterWritable cluster = new ClusterWritable();
		
		while (reader.next(cluster_id, cluster)) {
			
			Vector center = cluster.getValue().getCenter();
			Vector radius = cluster.getValue().getRadius();
			long age_l = Math.round((center.get(0) - radius.get(0))*10);
			long age_h = Math.round((center.get(0) + radius.get(0))*10);
 			long edu_l = Math.round(center.get(1) - radius.get(1));
 			long edu_h = Math.round(center.get(1) + radius.get(1));
			long income_l = Math.round((center.get(2) - radius.get(2)) * 10000);
			long income_h = Math.round((center.get(2) + radius.get(2)) * 10000);
			if (income_l < 0)
				income_l = 0;
			
			HashMap<Integer, Double> idx_val_car = new HashMap<Integer, Double>();
			double val;
			for (int i = car_base; i < state_base; i++) {
				val = center.get(i);
				idx_val_car.put(i, val);
			}
			String top3_car = getTopThree(idx_val_car);
			
			HashMap<Integer, Double> idx_val_state = new HashMap<Integer, Double>();
			for (int i = state_base; i < center.size(); i++) {
				val = center.get(i);
				idx_val_state.put(i, val);
			}
			String top3_state = getTopThree(idx_val_state);
	        
	        
	        String eduRangeString = eduMap.get(edu_l);
	        if (edu_l != edu_h) {
	        	eduRangeString += " ~ " + eduMap.get(edu_h);
	        }
	        
	        System.out.println("Cluster " + cluster_id.get() + 
	        		" (n = " + cluster.getValue().getNumObservations() + "):" + 
	        		"\n\t age = " + age_l + " ~ " + age_h +
	        		"\n\t edu = " + eduRangeString + 
	        		"\n\t income = " + income_l + " ~ " + income_h +
	        		"\n\t top states = " + top3_state +
	        		"\n\t top cars = " + top3_car);
	     
//	        dbg(cluster.getValue().toString());
			
		}
		reader.close();
	}
	
	private String getTopThree(HashMap<Integer, Double> map) {
		String s = "";
		
		ValueComparator vc =  new ValueComparator(map);
        TreeMap<Integer,Double> sorted_map = new TreeMap<Integer,Double>(vc);
        sorted_map.putAll(map);
        
        int i = 0;
        for(int idx: sorted_map.keySet()) {
        	if (i > 2) {break;}
        	
        	String descr = idx2descr.get(idx);
        	s += "\n\t\t " + descr + " -> " + map.get(idx);
        	i++;
        }
        return s;
	}

	private void generateDict() throws IOException {
		descr2idx = new HashMap<String,Integer>();
		idx2descr = new HashMap<Integer, String>();
		
		Path dictionaryPath = new Path("data/dict");
		FileSystem fs = FileSystem.get(dictionaryPath.toUri(), conf); 
		FileStatus[] outputFiles = fs.globStatus(new Path(dictionaryPath, "part-*"));

		List<String> features = Arrays.asList("age", "edu", "income");
		for (String s: features) {
			descr2idx.put(s, features.indexOf(s));
		}
		
		//add dimensions of cars
		int i = features.size();
		car_base = i;
		for (FileStatus fileStatus : outputFiles) {
			Path path = fileStatus.getPath();
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			Text key = new Text();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				descr2idx.put(key.toString(), i++);
			}
		}
		
		state_base = i;
		//add dimens of state
		for (String s: stateMap.values()) {
			descr2idx.put(s, i++);
		}
		
		vec_size = descr2idx.size();
		
		for (String s: descr2idx.keySet()){
			idx2descr.put(descr2idx.get(s), s);
//			System.out.println(s + " "+descr2idx.get(s));
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