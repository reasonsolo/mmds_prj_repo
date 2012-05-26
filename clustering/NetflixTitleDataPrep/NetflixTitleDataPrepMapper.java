//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class NetflixTitleDataPrepMapper extends MapReduceBase implements Mapper {

	public void map(WritableComparable key, Writable values,
			OutputCollector output, Reporter reporter) throws IOException {

		//MovieID,Year of movie release,Title of movie
		// We just ignore the year of release currently.
		String data = ((Text)values).toString();
		int index = data.indexOf(",");
		if(index==-1)
			return;
		String movie_id = data.substring(0, index);
		index = data.indexOf(",", index+1);
		if(index==-1)
			return;
		String title = data.substring(index+1);
		output.collect(new Text(movie_id), new Text(title+"\t"));
	}

}
