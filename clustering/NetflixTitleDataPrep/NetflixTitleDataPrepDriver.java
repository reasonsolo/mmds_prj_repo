//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;


public class NetflixTitleDataPrepDriver {

	// This mapreduce takes the text file mapping movieID's to movie Titles
	// and emits it so it can be used a sequence file.
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(NetflixTitleDataPrepDriver.class);

		conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class);
		conf.setJobName("NetflixTitleDataPrep");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputPath(new Path("src"));
		conf.setOutputPath(new Path("netflixTitles"));

		conf.setMapperClass(NetflixTitleDataPrepMapper.class);
		conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
