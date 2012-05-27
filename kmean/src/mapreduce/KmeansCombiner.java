package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import clusterer.KmeansCluster;

public class KmeansCombiner extends
		Reducer<LongWritable, KmeansCluster, LongWritable, KmeansCluster> {

	public void reduce(LongWritable key, Iterable<KmeansCluster> values,
			Context context) throws IOException {
		KmeansCluster cluster = new KmeansCluster((int) key.get());
		for (KmeansCluster point : values) {
			cluster.omitCluster(point);
		}
		try {
			context.write(key, cluster);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
