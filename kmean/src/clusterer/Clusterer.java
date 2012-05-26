package clusterer;

import java.util.ArrayList;
import java.util.HashMap;

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

	public void loadClusters() {
		// TODO read clusters data from last iteration.
	}

	public Cluster findNearestCluster(VectorDoubleWritable point)
			throws IllegalStateException {
		Cluster nearest = null;
		double mindist = Double.MAX_VALUE;
		double tempdist = 0;
		for (Cluster cluster : clusters) {
			tempdist = cluster.distance(point);
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
		this.clusters = clusters;
	}
}
