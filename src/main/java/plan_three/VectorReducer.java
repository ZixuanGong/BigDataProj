package plan_three;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class VectorReducer extends Reducer<Text,VectorWritable,Text,VectorWritable> {
	private VectorWritable writer = new VectorWritable();
	private static HashMap<String, SequentialAccessSparseVector> consumerUnitInfo;
	
	protected void reduce(Text id, Iterable<VectorWritable> values, Context context) 
			  										throws IOException,InterruptedException {
		//filter out the vectors where the cu info is not complete
		SequentialAccessSparseVector info_vec = consumerUnitInfo.get(id.toString());
		if (info_vec == null) {
			return;
		}
		
		Vector vector = null;
		for (VectorWritable partialVector : values) {
			if (vector == null) {
				vector = partialVector.get().like();
			}
			vector = vector.plus(partialVector.get());
		}
		vector = vector.plus(info_vec);
		
		NamedVector namedVector = new NamedVector(vector, id.toString());
		System.out.println(namedVector.toString());
		writer.set(namedVector);
		context.write(id, writer);
	}

	@Override
	protected void setup(
			Reducer<Text, VectorWritable, Text, VectorWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}
	
	public static void setCuInfo(HashMap<String,SequentialAccessSparseVector> map) {
		consumerUnitInfo = new HashMap<String, SequentialAccessSparseVector>();
		consumerUnitInfo = map;
		
		System.out.println("set cu info!!");
	}
	
	
}
