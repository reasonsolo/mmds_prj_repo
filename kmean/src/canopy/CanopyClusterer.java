package canopy;

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

public class CanopyClusterer {
	protected ArrayList<Canopy> canopies = new ArrayList<Canopy>();
	protected HashMap<Integer, Canopy> clusterMap = new HashMap<Integer, Canopy>();
	protected DistanceMeasure dm;
	protected double t1;
	protected double t2;
	protected int nextID;

	public CanopyClusterer() {
		canopies.clear();
		this.dm = new EuclideanDistance();
		nextID = 0;
	}

	public CanopyClusterer(DistanceMeasure dm, double t1, double t2) {
		canopies.clear();
		this.dm = dm;
		this.t1 = t1;
		this.t2 = t2;
		nextID = 0;
	}

	public CanopyClusterer(Configuration configuration) {
		// TODO Auto-generated constructor stub
		canopies.clear();
		this.t1 = Double.parseDouble( configuration.get(config.Constants.T1_KEY ) );
		this.t2 = Double.parseDouble( configuration.get(config.Constants .T2_KEY) );
		nextID = 0;
	}

	public void loadClusters(String clusterPath, Configuration conf) // need to
																		// be
																		// checked
			throws IOException, URISyntaxException {
		Path path = new Path(clusterPath);
		FileSystem fs = FileSystem.get(new URI(clusterPath), conf);

		SequenceFile.Reader reader = null;

		reader = new SequenceFile.Reader(fs, path, conf);
		IntWritable key = (IntWritable) ReflectionUtils.newInstance(
				reader.getKeyClass(), conf);
		Canopy value = (Canopy) ReflectionUtils.newInstance(
				reader.getValueClass(), conf);

		while (reader.next(key, value)) {
			canopies.add(value);
			nextID++;
		}
		IOUtils.closeStream(reader);
	}

	public Canopy findNearestCluster(VectorDoubleWritable point)
			throws IllegalStateException {
		Canopy nearest = null;
		double mindist = Double.MAX_VALUE;
		double tempdist = 0;
		for (Canopy cluster : canopies) {
			tempdist = cluster.euclideanDistance(point);
			if (tempdist < mindist) {
				mindist = tempdist;
				nearest = cluster;
			}
		}
		return nearest;
	}

	public boolean addPointToCanopies(VectorDoubleWritable point,
			ArrayList<Canopy> canopies) throws IllegalStateException {
		boolean flag = false;
		double tempdist = 0;
		for (Canopy canopy : canopies) {
			tempdist = canopy.euclideanDistance(point);
			if (tempdist < this.t1) {
				canopy.addPoint(point);
				flag = true;
			}
		}
		if (flag == false) {

			Canopy newCanopy = new Canopy(nextID++, point);
			canopies.add(newCanopy);
		}
		return flag;
	}

	public ArrayList<Canopy> getClusters() {
		return this.canopies;
	}
}
