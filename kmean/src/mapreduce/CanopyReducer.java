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
import config.ConfigConstants;
import distanceMeasure.DistanceMeasure;
import distanceMeasure.EuclideanDistance;


public class CanopyReducer extends
		Reducer<Text, VectorWritable, Text, Canopy>	{
	
	private final Collection<Canopy> canopies = new ArrayList<Canopy>();

	private CanopyCluster canopyCluster;

	CanopyCluster getCanopyClusterer() {
		return canopyClusterer;
	}
	
	
	@Override 
	protected void reduce (Text arg0, Iterable<VectorWritable> values, Context contex)
		throw IOException, InterruptedException	{

		for(VectorWritable value : vakues)	{
			canopyClusterer.addPointToCanopies(point, canopies);

		}
		
		for(Canopy canopy: canopies){
			context.write(new Text(canopy.getID() ) , canopy);
		}
	}

	@Override
	protected void setup(Context context) throw IOException, InterruptedExcetion	{
		super.setup(context);
		canopyCLusterer = new CanopyCluster ( context.getConfiguration());
	}
}


