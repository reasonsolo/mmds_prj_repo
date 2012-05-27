package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import clusterer.CanopyCluster;
import clusterer.CanopyClusterer;

import utils.VectorDoubleWritable;

public class CanopyMapper extends
		Mapper<IntWritable, Text, IntWritable, VectorDoubleWritable> {
	private VectorDoubleWritable point = null;
	protected CanopyClusterer canopyClusterer = new CanopyClusterer();
	private ArrayList<CanopyCluster> canopies = new ArrayList<CanopyCluster>();



	@Override
	protected void map(IntWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		System.out.println(values.toString());
		point = new VectorDoubleWritable(values);
		canopyClusterer.addPointToCanopies(point, canopies);

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		canopyClusterer = new CanopyClusterer(context.getConfiguration());
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (CanopyCluster canopy : canopies) { 
			context.write(new IntWritable(canopy.getId()),
					canopy.getCentroid());
		}
		super.cleanup(context);
	}
}
