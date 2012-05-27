package mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import clusterer.Cluster;
import clusterer.Clusterer;
import config.Constants;
import distanceMeasure.DistanceMeasure;
import distanceMeasure.EuclideanDistance;

public class KmeansReducer extends
		Reducer<IntWritable, Cluster, IntWritable, Cluster> {
	protected Clusterer clusterer;
	protected double threshold;
	Map<IntWritable, Cluster> clusterMap = new HashMap<IntWritable, Cluster>();

	@Override
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
			e.printStackTrace();
		}
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		threshold = conf.getFloat(Constants.THRESHOLD, 1);
		DistanceMeasure dm = null;
		try {
			dm = (DistanceMeasure) Class.forName(
					"distanceMeasure."
							+ conf.get(Constants.DISTANCE_MEASURE,
									"EuclideanDistance")).newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			dm = new EuclideanDistance();
			e.printStackTrace();
		}

		this.clusterer = new Clusterer(dm);

		String clusterPath = conf.get(Constants.CLUSTER_PATH);
		if (clusterPath != null && !clusterPath.isEmpty())
			try {
				this.clusterer.loadClusters(clusterPath, conf);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		context.getCounter(Constants.COUNTER_GROUP, Constants.COUNTER_TOTAL)
				.setValue(this.clusterer.getClusters().size());
	}
}
