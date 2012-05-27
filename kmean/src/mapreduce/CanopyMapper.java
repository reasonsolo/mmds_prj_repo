package mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

import utils.VectorDoubleWritable;
import clusterer.Cluster;
import clusterer.Clusterer;
import config.ConfigConstants;
import distanceMeasure.DistanceMeasure;
import distanceMeasure.EuclideanDistance;


public class CanopyMapper extends
		Mapper<IntWritable, Text, IntWritable, VectorDoubleWritable>	
{
	private VectorDoubleWritable point = null;
	protected CanopyClusterer canopyClusterer = new CanopyClusterer();
	private ArrayList<Canopy> canopies = new ArrayList<Canopy>();

	private void loadPart() {
		// TODO
		// a helper method, loads a single kmeans center part file
	}

	public void configure(JobConf conf) {
		// TODO:
	}
	
	@Override
	protected void map(Text key, VectorWritable point, Context context)
		throws IOException, InterruptedException 
	{
		canopyClusterer.addPointToCanopies(point, canopies);

	}

	
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

	



	
