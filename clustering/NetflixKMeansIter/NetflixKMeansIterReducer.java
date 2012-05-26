//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3 // import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class NetflixKMeansIterReducer extends MapReduceBase implements Reducer {

	
	private String status = "";
	
	// This will take in a list of vectors and output the average vector.
	// It also trims out users(dimensions) that don't occur frequently. Sad but true.
	public void reduce(WritableComparable key, Iterator values,
			OutputCollector output, Reporter reporter) throws IOException {
		
		
		int count = 0;
		TreeMap<Integer, Integer> features = new TreeMap<Integer, Integer>();
		TreeMap<Integer, Integer> userCounts = new TreeMap<Integer, Integer>();
		while(values.hasNext()) {
			count += 1;
			String data = ((Text)values.next()).toString();
			int index = data.indexOf(":");
			String movie_id = data.substring(0, index);
			data = data.substring(index+1);
			NetflixMovie curr = new NetflixMovie(movie_id, data);
			for(NetflixMovie.Triple t : curr.features) {
				if(!features.containsKey(t.userID)) {
					features.put(t.userID, new Integer(0));
					userCounts.put(t.userID, new Integer(0));
				}
				int new_count = userCounts.get(t.userID).intValue()+1;
				userCounts.put(t.userID, new_count);
				int new_value = features.get(t.userID)+t.rating;
				features.put(t.userID, new Integer(new_value));
			}
		}
				
		Iterator<Integer> it;
		int threshold = 0;
		int counter;
		// Iteratively increase the threshold until the number of features
		// is less than the ceiling - which I have magically chosen to be 100,000
		// Iniitally I was getting some clusters of size > 300,000 and then the mappers
		// were running out of memory in the next iteration. This could probably be optimized
		// around, but it is also probably fine to do this.
		while(true) {
			it = features.keySet().iterator();
			counter = 0;
			while (it.hasNext()) {
				Integer featureKey = it.next();
				Integer featureValue = features.get(featureKey);
				int div_count = userCounts.get(featureKey);
				int to_append = featureValue.intValue() / div_count;
				if ((to_append > 0) && (div_count > threshold))
					counter += 1;
				if(counter > 100000)
					break;
			}
			if(counter < 100000)
				break;
			else
				threshold++;
		}

		// Output the features that occur a minimum number of times.
		// We don't need to output any feature that averages to zero. That does
		// nothing for the cosine similarity function.
		StringBuilder builder = new StringBuilder();
		it = features.keySet().iterator();
		counter = 0;
		while (it.hasNext()) {
			Integer featureKey = it.next();
			Integer featureValue = features.get(featureKey);
			int div_count = userCounts.get(featureKey);
			int to_append = featureValue.intValue() / div_count;
			if ((to_append > 0) && (div_count > threshold)) {
				counter += 1;
				builder.append(featureKey);
				builder.append(",");
				builder.append(to_append);
				if (it.hasNext())
					builder.append(";");
			}
		}
		this.status += ":" + counter;
		reporter.setStatus(this.status);
		if (counter > 0) {
			String toOutput = builder.toString();
			output.collect(key, new Text(toOutput));
		}
	}

}
