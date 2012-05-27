package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.VectorDoubleWritable;
import clusterer.Cluster;

public class KmeansCombiner
		extends
		Reducer<IntWritable, Iterable<VectorDoubleWritable>, IntWritable, Cluster> {

	public void reduce(IntWritable key, Iterable<VectorDoubleWritable> values,
			Context context) throws IOException {
		Cluster cluster = new Cluster(key.get());
		for (VectorDoubleWritable point : values) {
			cluster.addPoint(point);
		}
		try {
			context.write(key, cluster);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
