package plan_one;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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


public class Clusterer {
	Configuration conf;
	FileSystem fs;
	static List points;
    
    // Write data to sequence files in Hadoop (write the vector to sequence file)
    public static void writePointsToFile(List<Vector> points, String fileName,
            							FileSystem fs,Configuration conf) throws IOException {
        
        Path path = new Path(fileName);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, 
        									LongWritable.class, VectorWritable.class);
        long recNum = 0;
        VectorWritable vec = new VectorWritable();
        
        for (Vector point : points) {
            vec.set(point);
            writer.append(new LongWritable(recNum++), vec);
        }
        
        writer.close();
    }
    
    public static void dbg(String msg) {
    	System.out.println(msg);
    }
    
    public static void getPointsFromFile() throws FileNotFoundException {
    	points = new ArrayList();
    	
    	BufferedReader br = null;
    	String line = "";
     
    	try {
    		br = new BufferedReader(new FileReader("assets/id_info.csv"));
    		while ((line = br.readLine()) != null) {
     
    			String[] tokens = line.split(",");
    			String id = tokens[0];
    			double[] tmp = new double[3];
    			tmp[0] = Integer.parseInt(tokens[1]);
    			tmp[0] /= 10;
    			tmp[1] = Integer.parseInt(tokens[2]);
    			tmp[2] = Integer.parseInt(tokens[3]);
    			tmp[2] /= 10000;
    			
    			//Car car = new Car(Integer.parseInt(tokens[4]), tokens[5]);
    			NamedVector vector = new NamedVector(new DenseVector(tmp), id);
                points.add(vector);
    		}
     
    	} catch (FileNotFoundException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		if (br != null) {
    			try {
    				br.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}
    }
    
    public Clusterer(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
		this.conf = conf;
		fs = FileSystem.get(conf);
		int k = 10;

//		getPointsFromFile();
//
//		File testData = new File("data_plan1");
//
//		if (!testData.exists()) {
//        	testData.mkdir();
//		}
//		testData = new File("data_plan1/points");
//		if (!testData.exists()) {
//            testData.mkdir();
//        }
//
//		writePointsToFile(points, "data_plan1/points/file1", fs, conf);
//		
//		createInitClusterCenters(k);
//          
//        KMeansDriver.run(conf, new Path("data_plan1/points"), 
//        		new Path("data_plan1/clusters"),
//        		new Path("data_plan1/output"), 
//        		0.001, 10, true, 0.1, false);
        
        printClusters(new Path("data_plan1/output/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-00000"));
    }

	private void printClusters(Path path) throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		IntWritable key = new IntWritable();        
		WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();

		while (reader.next(key, value)) {
	      	NamedVector vec = (NamedVector) value.getVector();
	      	double[] tmp = new double[3];
	      	tmp[0] = Math.round(vec.get(0) * 10);
				tmp[1] = Math.round(vec.get(1));
				tmp[2] = Math.round(vec.get(2) * 10000);
	      	
	      	Vector vec2 = new DenseVector(tmp);
	        System.out.println(vec2.toString() + " belongs to cluster " + key.toString());
		}
		reader.close();
	}

	private void createInitClusterCenters(int k) throws IOException {
		Path path = new Path("data_plan1/clusters/part-00000");
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Kluster.class);

		for (int i = 0; i < k; i++) {
            Vector vec = (Vector) points.get(i);
            
            Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
            writer.append(new Text(cluster.getIdentifier()), cluster);
        }
          
        writer.close();
	}
        
}
