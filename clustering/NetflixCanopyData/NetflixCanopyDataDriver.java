//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class NetflixCanopyDataDriver {

	// This map-reduce marks each movie vector with the canopies that it occurs in.
	// Each mapper load the list of canopies into memory during the configure method,
	// and then maps over the movie vector data. We use the identity reducer.
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(NetflixCanopyDataDriver.class);
		conf.setJobName("NetflixCanopyData");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setNumMapTasks(80);
		conf.setNumReduceTasks(80);
		
		conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);		
		conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class);
		
		//conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

        if (args.length < 2) {
            System.out.println("Usage: NetflixCanopyData <input path> <output path>");
            System.exit(0);
         }
         conf.setInputPath(new Path(args[0]));
         conf.setOutputPath(new Path(args[1]));

		conf.setMapperClass(NetflickCanopyDataMapper.class);
		//conf.setReducerClass(NetflixCanopyDataReducer.class);
		
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
