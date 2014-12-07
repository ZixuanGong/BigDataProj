package tagImpl;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class VectorReducer extends Reducer<Text,VectorWritable,Text,VectorWritable> {
	private VectorWritable writer = new VectorWritable();
	
	protected void reduce(Text tag, Iterable<VectorWritable> values, Context context) 
			  										throws IOException,InterruptedException {
		Vector vector = null;
		for (VectorWritable partialVector : values) {
			if (vector == null) {
				vector = partialVector.get().like();
			}
			vector = vector.plus(partialVector.get());
		}
		NamedVector namedVector = new NamedVector(vector, tag .toString());
		System.out.println(namedVector.toString());
		writer.set(namedVector);
		context.write(tag, writer);
	}
}
