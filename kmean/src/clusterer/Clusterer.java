package clusterer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import utils.VectorDoubleWritable;
import distanceMeasure.DistanceMeasure;
import distanceMeasure.EuclideanDistance;

public class Clusterer {
	protected ArrayList<Cluster> clusters = new ArrayList<Cluster>();
	protected HashMap<Integer, Cluster> clusterMap = new HashMap<Integer, Cluster>();
	protected DistanceMeasure dm;

	public Clusterer() {
		clusters.clear();
		this.dm = new EuclideanDistance();
	}

	public Clusterer(DistanceMeasure dm) {
		clusters.clear();
		this.dm = dm;
	}

	public void loadClusters(String clusterPath, Configuration conf)
			throws IOException, URISyntaxException {
		Path path = new Path(clusterPath);
		FileSystem fs = FileSystem.get(new URI(clusterPath), conf);

		SequenceFile.Reader reader = null;

		reader = new SequenceFile.Reader(fs, path, conf);
		IntWritable key = (IntWritable) ReflectionUtils.newInstance(
				reader.getKeyClass(), conf);
		Cluster value = (Cluster) ReflectionUtils.newInstance(
				reader.getValueClass(), conf);

		while (reader.next(key, value)) {
			clusters.add(value);
		}
		IOUtils.closeStream(reader);
	}

	public Cluster findNearestCluster(VectorDoubleWritable point)
			throws IllegalStateException {
		Cluster nearest = null;
		double mindist = Double.MAX_VALUE;
		double tempdist = 0;
		for (Cluster cluster : clusters) {
			tempdist = cluster.euclideanDistance(point);
			if (tempdist < mindist) {
				mindist = tempdist;
				nearest = cluster;
			}
		}
		return nearest;
	}

	public boolean isConverged(Cluster cluster, double threshold) {
		Cluster last = clusterMap.get(cluster.getId());
		return dm.distance(cluster.getCentroid(), last.getCentroid()) < threshold;
	}

	public ArrayList<Cluster> getClusters() {
		return this.clusters;
	}

	public void setClusters(ArrayList<Cluster> clusters) {
		this.clusters.clear();
		for (Cluster data : clusters) {
			this.clusters.add(data);
		}
	}
}
