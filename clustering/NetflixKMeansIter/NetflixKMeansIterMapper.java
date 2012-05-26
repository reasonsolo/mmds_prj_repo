//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class NetflixKMeansIterMapper extends MapReduceBase implements Mapper {

	
	private static HashMap<String, ArrayList<NetflixMovie> > meanCenters = new HashMap<String, ArrayList<NetflixMovie> >(); 
	private boolean done = false;
	private int count = 0;
	
	// First load the canopy centers into memory.
	// Then load the k-mean centers into memory.
	// For each k-mean center, store which canopies it is in.
	// Note that the k-mean centers are 10 different part files and we load them all.
	public void configure(JobConf conf) {
		try {
			if (done)
				return;
			done = true;
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/user/jhebert/out2/part-00000");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			ArrayList<NetflixMovie> centers = new ArrayList<NetflixMovie>();
			ArrayList<NetflixMovie> canopies = new ArrayList<NetflixMovie>(); Text key = new Text(); Text value = new Text();
			while (true) {
				reader.next(key, value);
				if (key.toString().equals(""))
					break;
				NetflixMovie curr = new NetflixMovie(key.toString(), value
						.toString());
				if (curr.features.size() > 5)
					canopies.add(curr);
				key.set("");
			}
			for(int i=0; i<10; i++)
				LoadPart(centers, conf, fs, i);
			for (NetflixMovie nm1 : canopies)
				for (NetflixMovie nm2 : centers) {
					if (nm1.MatchCount(nm2, 2) > 2) { // Kcenter is within the
														// canopy
						if (!meanCenters.containsKey(nm1.movie_id))
							meanCenters.put(nm1.movie_id,
									new ArrayList<NetflixMovie>());
						meanCenters.get(nm1.movie_id).add(nm2);
					}
				}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Loading a single k-mean-center part file.
	private void LoadPart(ArrayList<NetflixMovie> centers, JobConf conf,
			FileSystem fs, int num) {
		try {
			Path path = new Path(conf.get("kmeans") + "/part-0000" + num);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			Text key = new Text();
			Text value = new Text();
			key.set("");
			while (true) {
				reader.next(key, value);
				if (key.toString().equals(""))
					break;
				NetflixMovie curr = new NetflixMovie(key.toString(), value
						.toString());
				if (curr.features.size() > 5)
					centers.add(curr);
				key.set("");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	// cID:cID:..:movieID:featureData is value. movieID is key.
	// 
	// It should iterate through the list of mean centers and select only this
	// that
	// are inside of this canopy. Then it should emit each point in the canopy
	// to the mean that is closest to it.
	public void map(WritableComparable key, Writable values,
			OutputCollector output, Reporter reporter) throws IOException {
		count += 1;
		String status = count+":"+meanCenters.values().size();
		reporter.setStatus(status);
		//String movie_id = ((Text)key).toString();
		String data = ((Text)values).toString();
		String[] items = data.split(":");
		NetflixMovie curr = new NetflixMovie(items[items.length-2], items[items.length-1]);
	
		
		HashSet<NetflixMovie> done = new HashSet<NetflixMovie>();
		double maxDist = -1;
		NetflixMovie maxMovie = null;
		for(int i=0; i<items.length-2; i++) {
			String cID = items[i];
			ArrayList<NetflixMovie> movies = meanCenters.get(cID);
			if(movies==null)
				continue;
			for(NetflixMovie nm : movies){
				if(done.contains(nm))
					continue;
				done.add(nm);

				double dist;
				dist = nm.ComplexDistance(curr);
				if(dist > maxDist) {
					maxDist = dist;
					maxMovie = nm;
				}
			}
		}
		if(maxDist > -1) {
			// Only need to output the movie vector as the value and the k-mean-center
			// that is closest as the key.
			String to_emit = items[items.length-2]+":"+items[items.length-1];
			output.collect(new Text(maxMovie.movie_id), new Text(to_emit));
		}
	}

}
