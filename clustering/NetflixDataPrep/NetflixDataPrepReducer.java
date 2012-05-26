//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import java.lang.StringBuilder;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class NetflixDataPrepReducer extends MapReduceBase implements Reducer {

	public void reduce(WritableComparable key, Iterator values,
			OutputCollector output, Reporter reporter) throws IOException {

		// Key will be movieID
		// Values will be Text of userID,rating,time
		// Want to output all tuples seperated by semicolon
		
		TreeMap<Integer, String> features = new TreeMap<Integer, String>();
		while(values.hasNext()) {
			String[] items = (((Text)values.next()).toString()).split(",");
			features.put(new Integer(items[0]), items[1]);			
		}
		
		StringBuilder builder = new StringBuilder();
		Iterator<Integer> it = features.keySet().iterator();
		while(it.hasNext()) {
			Integer featureKey = it.next();
			String featureValue = features.get(featureKey);
			builder.append(featureKey);
			builder.append(",");
			builder.append(featureValue);
			if(it.hasNext())
				builder.append(";");
		}
		
		String toOutput = builder.toString();
		output.collect(key, new Text(toOutput));
	}
}
