package mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import canopy.*;

import utils.VectorDoubleWritable;

import config.Constants;
import distanceMeasure.DistanceMeasure;
import distanceMeasure.EuclideanDistance;

public class CanopyReducer extends
		Reducer<IntWritable, VectorDoubleWritable, Text, Canopy>	{
	
	private final ArrayList<Canopy> canopies = new ArrayList<Canopy>();

	private CanopyClusterer canopyClusterer;

	CanopyClusterer getCanopyClusterer() 
		{
		return canopyClusterer;
		}
		
	
	
	@Override 
	public void reduce (IntWritable key, Iterable<VectorDoubleWritable> values, Context context)
		throws IOException, InterruptedException	{

		for(VectorDoubleWritable value : values)	{
			canopyClusterer.addPointToCanopies(value, canopies);

		}
		
		for(Canopy canopy: canopies){
			context.write(new Text(canopy.getId().toString() ) , canopy);
		}
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException	{
		super.setup(context);
		canopyClusterer = new CanopyClusterer ( context.getConfiguration());
	}
}


