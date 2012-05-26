package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import clusterer.Cluster;
import clusterer.Clusterer;

public class KmeansReducer extends
		Reducer<IntWritable, Cluster, IntWritable, Cluster> {
	protected Clusterer clusterer;
	protected final double threshold = 1.0;

	Map<IntWritable, Cluster> clusterMap = new HashMap<IntWritable, Cluster>();

	public void reduce(IntWritable key, Iterable<Cluster> values,
			Context context) throws IOException {
		Cluster cluster = clusterMap.get(key);
		for (Cluster value : values) {
			cluster.omitCluster(value);
		}

		if (clusterer.isConverged(cluster, threshold))
			context.getCounter("Clusterer", "Converged Cluster").increment(1);
		try {
			context.write(key, cluster);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// TODO:
		// The reducer should output the (partial) result anyway...
	}
}
