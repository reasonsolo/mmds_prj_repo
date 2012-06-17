package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import clusterer.CanopyCluster;
import clusterer.CanopyClusterer;

import vector.VectorDoubleWritable;
import config.Constants;

public class CanopyMapper extends
		Mapper<LongWritable, Text, Text, VectorDoubleWritable> {
	private VectorDoubleWritable point = null;
	protected CanopyClusterer canopyClusterer = new CanopyClusterer();
	private ArrayList<CanopyCluster> canopies = new ArrayList<CanopyCluster>();



	@Override
	protected void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		//System.out.println(values.toString());
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
			if (canopy.getSize() >= Constants.CANOPY_MIN_NUMBER) {
				context.write(//new Text(canopy.getId().toString()),
					new Text("1"), canopy.getCentroid());
			}
		}
		super.cleanup(context);
	}
}
