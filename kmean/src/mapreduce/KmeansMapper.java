package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

import utils.VectorDoubleWritable;
import clusterer.Cluster;
import clusterer.Clusterer;

public class KmeansMapper extends
		Mapper<IntWritable, Text, IntWritable, VectorDoubleWritable> {
	private VectorDoubleWritable point = null;
	protected Clusterer clusterer = new Clusterer();
    
    private void loadPart() {
        // TODO 
        // a helper method, loads a single kmeans center part file
    }

    public void configure(JobConf conf) {
        // TODO:
        // This method should load all canopy centers and k-mean centers
        // into memory, and get the mapper ready for iteration
    }


	public void map(IntWritable key, Text values, Context context)
			throws IOException {
		point = new VectorDoubleWritable(values);

		Cluster cluster = null;
		try {
			cluster = clusterer.findNearestCluster(point);
			context.write(new IntWritable(cluster.getId()), point);
		} catch (IllegalStateException e) {
			System.err.println("Error:\t" + e.getMessage() + " at row(" + key
					+ ")");
		} catch (InterruptedException e) {
			// TODO
			e.printStackTrace();
		}
	}

}
