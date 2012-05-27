package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.VectorDoubleWritable;
import clusterer.KmeansCluster;

public class KmeansCombiner
		extends
		Reducer<IntWritable, Iterable<VectorDoubleWritable>, IntWritable, KmeansCluster> {

	public void reduce(IntWritable key, Iterable<VectorDoubleWritable> values,
			Context context) throws IOException {
		KmeansCluster cluster = new KmeansCluster(key.get());
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
