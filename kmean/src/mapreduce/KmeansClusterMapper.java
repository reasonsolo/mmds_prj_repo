package mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.VectorDoubleWritable;
import clusterer.Cluster;
import clusterer.Clusterer;
import config.ConfigConstants;
import distanceMeasure.DistanceMeasure;
import distanceMeasure.EuclideanDistance;

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

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		DistanceMeasure dm = null;
		try {
			dm = (DistanceMeasure) Class.forName(
					"distanceMeasure."
							+ conf.get(ConfigConstants.DISTANCE_MEASURE,
									"EuclideanDistance")).newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			dm = new EuclideanDistance();
			e.printStackTrace();
		}

		this.clusterer = new Clusterer(dm);

		String clusterPath = conf.get(ConfigConstants.CLUSTER_PATH);
		if (clusterPath != null && !clusterPath.isEmpty())
			try {
				this.clusterer.loadClusters(clusterPath, conf);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
	}
}
