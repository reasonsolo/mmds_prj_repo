package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import clusterer.CanopyCluster;
import clusterer.CanopyClusterer;
import clusterer.KmeansCluster;
import vector.VectorDoubleWritable;
import config.Constants;

public class CanopyReducer extends
		Reducer<Text, VectorDoubleWritable, LongWritable, KmeansCluster> {

	private final ArrayList<CanopyCluster> canopies = new ArrayList<CanopyCluster>();
	private CanopyClusterer canopyClusterer;

	CanopyClusterer getCanopyClusterer() {
		return canopyClusterer;
	}

	@Override
	public void reduce(Text key, Iterable<VectorDoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		for (VectorDoubleWritable value : values) {
			//System.out.println("1\n");
			//value.print();
			canopyClusterer.addPointToCanopies(value, canopies);
		}
		
		for (CanopyCluster canopy: this.canopies) {
			KmeansCluster kcluster = new KmeansCluster(canopy.getId(), 
						canopy.getS1(), canopy.getS2());
			System.out.println(canopy.getId().toString() 
							+ "\t" + canopy.getCentroid().toString());
			context.write(new LongWritable(kcluster.getId()), kcluster); 
		}
	
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		canopyClusterer = new CanopyClusterer();
		canopyClusterer.useT3T4(conf.getFloat(Constants.T3_KEY, 0.1f),
					conf.getFloat(Constants.T4_KEY, 0.2f));
	}
}