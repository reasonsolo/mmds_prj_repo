package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.VectorDoubleWritable;
import Canopy.Canopy;

public class CanopyCombiner
		extends
		Reducer<IntWritable, Iterable<VectorDoubleWritable>, IntWritable, Canopy> {

	public void reduce(IntWritable key, Iterable<VectorDoubleWritable> values,
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
