//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NetflixCanopyAmalagator extends MapReduceBase implements Reducer {

	
	// So we will have one single reducer and it maintains a list of the final canopy centers.
	// Each center emitted from the mappers will only be added if none of the current centers
	// covers it (this same canopy selection algorithm as before, and in the assigned reading).

	// NOTE: This is a mostly working algorithm and definitely worked for me in practice, though no
	// proof is attached. There might technically be a data point that is not selected as a canopy center
	// but is not within the near distance of any canopy center due to the 2-level map-reduce selection.
	// However, I argue that this still works as a k-means canopy selector and is not a fundamental flaw.
	// To fix this you would just need to limit yourself to a single mapper, or get tricker about also
	// emitting points belonging to a canopy to the reducer.
	
	private int count = 0;
	private ArrayList<NetflixMovie> canopyCenters;
	
	
	public void configure(JobConf conf) {
		this.canopyCenters = new ArrayList<NetflixMovie>();
	}

	// If we have multiple mappers then we might have to be tricky about
	// selecting disjoint canopy centers.
	public void reduce(WritableComparable key, Iterator values,
			OutputCollector output, Reporter reporter) throws IOException {

		this.count += 1;
		//String canopy_id = ((Text) key).toString();
		while (values.hasNext()) {
			String data = ((Text) values.next()).toString();
			int index = data.indexOf(":");
			String movie_id = data.substring(0, index);
			data = data.substring(index+1);
			NetflixMovie curr = new NetflixMovie(movie_id, data);
			boolean too_close = false;
			
			for (NetflixMovie nm : this.canopyCenters) {
				int matchCount = nm.MatchCount(curr);
				if(matchCount > 10) {
					too_close = true;
					break;
				}
			}
			if (!too_close) {
				Text to_emit = new Text(data);
				output.collect(new Text(curr.movie_id), to_emit);
				this.canopyCenters.add(curr);
				String toShow = this.canopyCenters.size() + ":" + this.count;
				reporter.setStatus(toShow);
			}
		}
	}
}
