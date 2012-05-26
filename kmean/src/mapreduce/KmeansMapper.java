package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class KmeansMapper extends MapReduceBase implements
		Mapper<WritableComparable<?>, Writable, OutputCollector, Reporter> {

	@Override
	public void map(WritableComparable key, Writable values,
			OutputCollector output, Reporter reporter) throws IOException {
	}
}
