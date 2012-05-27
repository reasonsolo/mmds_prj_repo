package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.VectorDoubleWritable;
import canopy.Canopy;
import canopy.CanopyClusterer;

public class CanopyMapper extends
		Mapper<IntWritable, Text, IntWritable, VectorDoubleWritable> {
	private VectorDoubleWritable point = null;
	protected CanopyClusterer canopyClusterer = new CanopyClusterer();
	private ArrayList<Canopy> canopies = new ArrayList<Canopy>();



	@Override
	protected void map(IntWritable key, Text values, Context context)
			throws IOException, InterruptedException {
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
		for (Canopy canopy : canopies) { 
			context.write(new IntWritable(canopy.getId()),
					canopy.getCentroid());
		}
		super.cleanup(context);
	}
}
