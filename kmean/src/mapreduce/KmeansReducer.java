package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KmeansReducer extends MapReduceBase implements
		Reducer<WritableComparable<?>, Iterator, OutputCollector, Reporter> {

	public void reduce(WritableComparable _key, Iterator values,
			OutputCollector output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		Text key = (Text) _key;

		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			Integer value = (Integer) values.next();

			// process value
		}
	}

}
