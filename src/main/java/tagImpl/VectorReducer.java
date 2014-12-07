package tagImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class VectorReducer extends Reducer<Text,VectorWritable,Text,VectorWritable> {
	private VectorWritable writer = new VectorWritable();
	private HashMap<String, DenseVector> consumerUnitInfo;
	
	protected void reduce(Text id, Iterable<VectorWritable> values, Context context) 
			  										throws IOException,InterruptedException {
		Vector vector = null;
		for (VectorWritable partialVector : values) {
			if (vector == null) {
				vector = partialVector.get().like();
			}
			vector = vector.plus(partialVector.get());
		}
		DenseVector info_vec = consumerUnitInfo.get(id.toString());
		vector.set(2, info_vec.get(2));
		vector.set(1, info_vec.get(1));
		vector.set(0, info_vec.get(0));
		
		NamedVector namedVector = new NamedVector(vector, id.toString());
		System.out.println(namedVector.toString());
		writer.set(namedVector);
		context.write(id, writer);
	}

	@Override
	protected void setup(
			Reducer<Text, VectorWritable, Text, VectorWritable>.Context context)
			throws IOException, InterruptedException {
		getCuInfo();
		super.setup(context);
	}
	
	private void getCuInfo() throws IOException {
		consumerUnitInfo = new HashMap<String, DenseVector>();
		
		BufferedReader br = null;
    	String line = "";
     
		br = new BufferedReader(new FileReader("assets/id_info.csv"));
		while ((line = br.readLine()) != null) {
 
			String[] tokens = line.split(",");
			String id = tokens[0];
			try {
				double age = Double.parseDouble(tokens[1]);
				double edu = Double.parseDouble(tokens[2]);
				double income = Double.parseDouble(tokens[3]);
				
				DenseVector vector = new DenseVector(new double[]{age/10, edu, income/10000});
				consumerUnitInfo.put(id, vector);
				
			} catch (NumberFormatException e) {
				continue;
			}
		}
		br.close();
	}
	
}
