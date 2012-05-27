package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

import utils.VectorDoubleWritable;
import Canopy.Canopy;
import Canopy.CanopyClusterer;

public class CanopyMapper extends
		Mapper<Text, VectorDoubleWritable, IntWritable, VectorDoubleWritable> {
	private VectorDoubleWritable point = null;
	protected CanopyClusterer canopyClusterer = new CanopyClusterer();
	private ArrayList<Canopy> canopies = new ArrayList<Canopy>();

	private void loadPart() {
		// TODO
		// 
	}

	public void configure(JobConf conf) {
		// TODO:
	}

	@Override
	protected void map(Text key, VectorDoubleWritable point, Context context)
			throws IOException, InterruptedException {
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
			context.write(new Text("centroid"),
					new VectorWritable(canopy.center()));
		}
		super.cleanup(contex);
	}
}
