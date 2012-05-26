//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class NetflixKMeansIterDriver {

	
	// This technically has the viewer map-reduce rolled into it as we need the same mapper task
	// to determine cluster membership.
	
	// We map over the canopy annotated movieVectors and load the current k-centers into memory
	// in each mapper. 
	// The reduce stage then determines the new cluster centers.
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(NetflixKMeansIterDriver.class);
		conf.setJobName("NetflixKMeansIter");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		
		conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
		// Comment out these two lines for the viewer map-reduce.
		conf.setNumReduceTasks(10);
		conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class);
		
		if (args.length < 2) {
			System.out
					.println("Usage: NetflixKMeansIter <input path> <output path>");
			System.exit(0);
		}
		// Input mapping data is always the canopy annoted movieVectors
		conf.setInputPath(new Path("out3"));
		conf.setOutputPath(new Path(args[1]));

		// Set the location of current kmean centers for the mapper tasks.
		conf.set("kmeans", args[0]);

		conf.setMapperClass(NetflixKMeansIterMapper.class);
		// Switch the reducer depending on the job being run.
		conf.setReducerClass(NetflixKMeansIterReducer.class);
		//conf.setReducerClass(NetflixKMeansIterViewer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
