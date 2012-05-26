//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.io.IOException;
import java.util.*;
import java.lang.StringBuilder;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;		
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class NetflickCanopyDataMapper extends MapReduceBase implements Mapper {

	private static ArrayList<NetflixMovie> capopyCenters = new ArrayList<NetflixMovie>();
	private boolean done = false;
	private int count = 0;
	
	// Load the canopy centers into memory.
	public void configure(JobConf conf) {
		
	     try {
	    	 if(done)
	    		 return;
	    	 else
	    		 done = true;
             FileSystem fs = FileSystem.get(conf);
             Path path = new Path("/user/jhebert/out2/part-00000");
             SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
             Text key = new Text();
             Text value = new Text();
             while(true) {
            	 reader.next(key, value);
            	 if(key.toString().equals(""))
            		 break;
     			 NetflixMovie curr = new NetflixMovie(key.toString(), value.toString());
     			 capopyCenters.add(curr);
     			 key.set("");
             }
     } catch (IOException e) {
              e.printStackTrace();
     }
	}
	
	// This needs to read through the netflix data and for each point if it belongs
	// to a canopy, then emit it to it.
	// Final form will be CanopyID1:CanopyID2:... MovieID:movieVector
	// where movieVector contains the userID,rating pairs.
	public void map(WritableComparable key, Writable values,
			OutputCollector output, Reporter reporter) throws IOException {
		count += 1;
		String movie_id = ((Text)key).toString();
		String data = ((Text)values).toString();
		String status = count+":"+capopyCenters.size()+":"+movie_id;
		reporter.setStatus(status);

		NetflixMovie curr = new NetflixMovie(movie_id, data);
		boolean emitted = false;
		StringBuilder builder = new StringBuilder();
		for(NetflixMovie center: capopyCenters) {
			int matchCount = curr.MatchCount(center, 2);
			if(matchCount > 2) {
				emitted |= true;
				if(builder.length()>0)
					builder.append(":");
				builder.append(center.movie_id);
			}
		}
		if(emitted) {
			builder.append(":");
			builder.append(movie_id);
			builder.append(":");
			builder.append(data);
			String to_emit = builder.toString();
			output.collect(new Text(movie_id), new Text(to_emit));
		}else
			reporter.setStatus("Did not emit: "+movie_id);
	}

}
