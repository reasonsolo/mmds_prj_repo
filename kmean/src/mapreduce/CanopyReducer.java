package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import clusterer.CanopyCluster;
import clusterer.CanopyClusterer;

import utils.VectorDoubleWritable;

public class CanopyReducer extends
		Reducer<LongWritable, VectorDoubleWritable, Text, CanopyCluster> {

	private final ArrayList<CanopyCluster> canopies = new ArrayList<CanopyCluster>();

	private CanopyClusterer canopyClusterer;

	CanopyClusterer getCanopyClusterer() {
		return canopyClusterer;
	}

	@Override
	public void reduce(LongWritable key, Iterable<VectorDoubleWritable> values,
			Context context) throws IOException, InterruptedException {

		for (VectorDoubleWritable value : values) {
			canopyClusterer.addPointToCanopies(value, canopies);

		}

		for (CanopyCluster canopy : canopies) {
			context.write(new Text(canopy.getId().toString()), canopy);
		}
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		canopyClusterer = new CanopyClusterer(context.getConfiguration());
	}
}