package mapreduce;

import java.io.IOException;
<<<<<<< HEAD
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
=======
import java.util.ArrayList;

>>>>>>> bc35d6cea01417baa0256b7d314c893a4da96e10
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

import utils.VectorDoubleWritable;
<<<<<<< HEAD
import clusterer.Cluster;
import clusterer.Clusterer;
import config.ConfigConstants;
import distanceMeasure.DistanceMeasure;
import distanceMeasure.EuclideanDistance;


public class CanopyMapper extends
		Mapper<IntWritable, Text, IntWritable, VectorDoubleWritable>	
{
=======
import Canopy.Canopy;
import Canopy.CanopyClusterer;

public class CanopyMapper extends
		Mapper<Text, VectorDoubleWritable, IntWritable, VectorDoubleWritable> {
>>>>>>> bc35d6cea01417baa0256b7d314c893a4da96e10
	private VectorDoubleWritable point = null;
	protected CanopyClusterer canopyClusterer = new CanopyClusterer();
	private ArrayList<Canopy> canopies = new ArrayList<Canopy>();

	private void loadPart() {
		// TODO
<<<<<<< HEAD
		// a helper method, loads a single kmeans center part file
=======
		// 
>>>>>>> bc35d6cea01417baa0256b7d314c893a4da96e10
	}

	public void configure(JobConf conf) {
		// TODO:
	}
<<<<<<< HEAD
	
	@Override
	protected void map(Text key, VectorWritable point, Context context)
		throws IOException, InterruptedException 
	{
=======

	@Override
	protected void map(Text key, VectorDoubleWritable point, Context context)
			throws IOException, InterruptedException {
>>>>>>> bc35d6cea01417baa0256b7d314c893a4da96e10
		canopyClusterer.addPointToCanopies(point, canopies);

	}

<<<<<<< HEAD
	
	@Override 
	protected void setup (Context context) throw IOException , InterruptedException	{
		super.setup(contex);
		canopyClusterer = new CanopyClusterer(contex.getConfiguration());
	}

	@Override 
	protected void cleanup ( Context context ) throw IOException , InterruptedException	{
		for(Canopy canopy: canopies) {
			context.write(new Text("centroid"), new VectorWritable(canopy.center() ) );
			}
		super.cleanup(contex);
	}
}

	



	
=======
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		canopyClusterer = new CanopyClusterer(context.getConfiguration());
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Canopy canopy : canopies) {
			context.write(new Text("centroid"),
					new VectorWritable(canopy.center()));
		}
		super.cleanup(contex);
	}
}
>>>>>>> bc35d6cea01417baa0256b7d314c893a4da96e10
