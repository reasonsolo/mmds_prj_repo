package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.VectorDoubleWritable;
<<<<<<< HEAD
import clusterer.Cluster;


public class CanopyCombiner
		extends
		Reducer<IntWritable, Iterable<VectorDoubleWritable>, IntWritable, CanopyCluster> {

	public void reduce(IntWritable key, Iterable<VectorDoubleWritable> values,
			Context context) throws IOException {
		CanopyCluster canopy = new CanopyCluster();
=======
import Canopy.Canopy;

public class CanopyCombiner
		extends
		Reducer<IntWritable, Iterable<VectorDoubleWritable>, IntWritable, Canopy> {

	public void reduce(IntWritable key, Iterable<VectorDoubleWritable> values,
			Context context) throws IOException {
		Canopy canopy = new Canopy();
>>>>>>> bc35d6cea01417baa0256b7d314c893a4da96e10
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
