package clusterer;

import java.util.ArrayList;

import utils.VectorDoubleWritable;

public class Clusterer {
	protected ArrayList<Cluster> clusters = new ArrayList<Cluster>();

	public Clusterer() {
		clusters.clear();
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

	public ArrayList<Cluster> getClusters() {
		return this.clusters;
	}

	public void setClusters(ArrayList<Cluster> clusters) {
		this.clusters = clusters;
	}
}
