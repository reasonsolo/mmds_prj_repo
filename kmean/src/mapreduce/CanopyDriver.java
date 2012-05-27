package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


import canopy.*;
import config.Constants;


public class CanopyDriver {
	public static void configure() {

	}
	
	public static void main(String[] args){
	
		if (args.length < 5) {
			System.out.println("Usage: " + args[0] + " <t1> <t2> <input> <output>");
			System.exit(0);
		}
	
		Configuration conf = new Configuration();
		Path in = new Path(args[3]);
		Path out = new Path(args[4]);
		
		Counter converge = null;
		Counter total = null;
		
		try {

			while (converge != total) {
				Job job = new Job(conf);
				job.setNumReduceTasks(2);
				job.setJobName("Canopy clustering");

				job.setMapperClass(CanopyMapper.class);
				job.setReducerClass(CanopyReducer.class);
				job.setJarByClass(CanopyDriver.class);

				SequenceFileInputFormat.addInputPath(job, in);
				SequenceFileOutputFormat.setOutputPath(job, out);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);

				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Canopy.class);

				job.waitForCompletion(true);

				converge = job.getCounters().getGroup(Constants.COUNTER_GROUP)
						.findCounter(Constants.COUNTER_CONVERGED);
				total = job.getCounters().getGroup(Constants.COUNTER_GROUP)
						.findCounter(Constants.COUNTER_TOTAL);
			}

		} catch (Exception e) {
			// TODO:
			// a better error report routine
			e.printStackTrace();
		}
		
	}
	
}

