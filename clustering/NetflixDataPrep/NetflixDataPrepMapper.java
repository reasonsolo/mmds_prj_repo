//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class NetflixDataPrepMapper extends MapReduceBase implements Mapper {

	public void map(WritableComparable key, Writable values,
			OutputCollector output, Reporter reporter) throws IOException {
	
		// input will be line of data movieId,userId,rating,time
		// output (moiveId, userId,rating,time)
		String line = ((Text)values).toString();
		String[] items = line.split(",");
		if(items.length != 4) {
			reporter.setStatus("Doesn't work:"+line);
			return;
		}
		Text keyToEmit = new Text(items[0]);
		Text valueToEmit = new Text(items[1]+","+items[2]);
		output.collect(keyToEmit, valueToEmit);
	}

}
