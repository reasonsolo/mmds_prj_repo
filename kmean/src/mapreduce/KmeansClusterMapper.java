package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.VectorDoubleWritable;
import clusterer.Cluster;
import clusterer.Clusterer;

public class KmeansClusterMapper extends
		Mapper<IntWritable, Text, IntWritable, IntWritable> {
	private VectorDoubleWritable point = null;
	protected Clusterer clusterer = new Clusterer();

	public void map(IntWritable key, Text values, Context context)
			throws IOException {
		point = new VectorDoubleWritable(values);

		Cluster cluster = null;
		try {
			cluster = clusterer.findNearestCluster(point);
			// TODO Find a proper way to represent the clustering result.
			context.write(new IntWritable(cluster.getId()), key);
		} catch (IllegalStateException e) {
			System.err.println("Error:\t" + e.getMessage() + " at row(" + key
					+ ")");
		} catch (InterruptedException e) {
			// TODO
			e.printStackTrace();
		}
	}

}
