package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.VectorDoubleWritable;
import clusterer.Cluster;


public class CanopyCombiner
		extends
		Reducer<IntWritable, Iterable<VectorDoubleWritable>, IntWritable, CanopyCluster> {

	public void reduce(IntWritable key, Iterable<VectorDoubleWritable> values,
			Context context) throws IOException {
		CanopyCluster canopy = new CanopyCluster();
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
