//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class NetflixCanopySelector extends MapReduceBase implements Mapper {

	// We maintain a list of current canopy centers. We then iterate over each point
	// presented to this mapper. If it is not within a minimum distance of any current canopy
	// center, then it is a new canopy center and is emitted to the reducer. This does no marking
	// of points being in canopies, that is a later map-reduce.
	
	private int count = 0;
	private ArrayList<NetflixMovie> canopyCenters;
	
	public void configure(JobConf job) {
		this.canopyCenters = new ArrayList<NetflixMovie>();
	}
	
	// input:  key is movideID, value is <userID,ranking> tuple
	// output: movieID of canopy center, movieID: <userID, ranking> 
	public void map(WritableComparable key, Writable values,
			OutputCollector output, Reporter reporter) throws IOException {
		this.count += 1;
		String movie_id = ((Text)key).toString();
		String data = ((Text)values).toString();
		NetflixMovie curr = new NetflixMovie(movie_id, data);
		boolean too_close = false;
		Text to_emit = new Text(curr.movie_id+":"+data);
		for(NetflixMovie nm: this.canopyCenters) {
			int matchCount = nm.MatchCount(curr);
			if(matchCount > 10)
				too_close = true;
		}
		if(! too_close) {
			output.collect(new Text(curr.movie_id), to_emit);
			this.canopyCenters.add(curr);
			String toShow = this.canopyCenters.size()+":"+this.count;
			reporter.setStatus(toShow);
		}
	}
}
