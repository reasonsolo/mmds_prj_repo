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
		System.out.println("Combiner cluster:\t" + cluster.getId());
		for (KmeansCluster value : values) {
			System.out.println("Combiner Values " + value.getId() + ":\t"
					+ value.getSize() + "\t" + value.getS1().size() + "\t"
					+ value.getS1().get());
			cluster.omitCluster(value);
		}
		System.out.println("FINISH");
		try {
			context.write(key, cluster);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
