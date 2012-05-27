/*package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.VectorDoubleWritable;
import canopy.Canopy;

public class CanopyCombiner
		extends
		Reducer<LongWritable, Iterable<VectorDoubleWritable>, LongWritable, Canopy> {

	public void reduce(LongWritable key, Iterable<VectorDoubleWritable> values,
			Context context) throws IOException {
		Canopy canopy = new Canopy();
		for (VectorDoubleWritable point : values) {
			canopy.addPoint(point);
		}
		try {
			context.write(key, canopy);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
*/