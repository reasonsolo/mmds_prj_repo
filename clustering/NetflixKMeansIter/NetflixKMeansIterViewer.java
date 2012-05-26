//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.io.IOException;
import java.lang.StringBuilder;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class NetflixKMeansIterViewer extends MapReduceBase implements Reducer {

	private static HashMap<String, String> movieTitles = new HashMap<String, String>();
	private boolean done = false;
	
	// Load the translation between movieID's and movie titles. This easily fits into memory.
	public void configure(JobConf conf) {
		if(done)
			return;
		done = true;
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/user/jhebert/netflixTitles/part-00000");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			Text key = new Text("");
			Text value = new Text();
			int count=0;
			while(true) {
				count += 1;
				reader.next(key, value);
				if(key.toString().equals(""))
					break;
				movieTitles.put(key.toString(), value.toString());
				key.set("");
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	// So for this reduce we just want to output all of the titles in one cluster	
	public void reduce(WritableComparable key, Iterator values,
			OutputCollector output, Reporter reporter) throws IOException {
		int count = 0;
		StringBuilder builder = new StringBuilder();
		while(values.hasNext()) {
			count += 1;
			if(count > 10000)
				break;
			String data = ((Text)values.next()).toString();
			int index = data.indexOf(":");
			String movie_id = data.substring(0, index);
			if(movieTitles.containsKey(movie_id)) {
				builder.append(movieTitles.get(movie_id));
				builder.append("\t");
			}
		}
		output.collect(key, new Text(builder.toString()));
	}

}
