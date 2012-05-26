package clusterer;

import java.util.ArrayList;

import utils.VectorDoubleWritable;

public class Clusterer {
	private ArrayList<Cluster> clusters = new ArrayList<Cluster>();

	public Clusterer() {
		clusters.clear();
	}

	public void loadClusters() {

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
}
