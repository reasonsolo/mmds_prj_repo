package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import clusterer.Cluster;

public class KmeansReducer extends
		Reducer<IntWritable, Cluster, IntWritable, Cluster> {

	Map<IntWritable, Cluster> clusterMap = new HashMap<IntWritable, Cluster>();

	public void reduce(IntWritable key, Iterable<Cluster> values,
			Context context) throws IOException {
		Cluster cluster = clusterMap.get(key);
		for (Cluster value : values) {
			cluster.omitCluster(value);
		}
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
