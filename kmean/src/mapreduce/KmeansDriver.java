package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import clusterer.Cluster;
import config.Constants;

public class KmeansDriver {
	public static void configure() {

	}

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Usage: " + args[0] + "<input> <output>");
			System.exit(0);
		}

		Configuration conf = new Configuration();

		Counter converge = null;
		Counter total = null;
		Path in;
		Path out;
		int iterCounter = 0;
		try {
			while (iterCounter == 0 || converge != total) {
				Job job = new Job(conf);
				job.setNumReduceTasks(2);
				job.setJobName("K-means clustering");

				job.setMapperClass(KmeansMapper.class);
				job.setReducerClass(KmeansReducer.class);
				job.setJarByClass(KmeansDriver.class);

				if (iterCounter == 0)
					in = new Path(args[1]);
				else
					// load the output of last iteration
					in = new Path(args[1] + ".part" + (iterCounter - 1));
				out = new Path(args[1] + ".part" + iterCounter);
				SequenceFileInputFormat.addInputPath(job, in);
				SequenceFileOutputFormat.setOutputPath(job, out);

				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);

				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Cluster.class);

				job.waitForCompletion(true);

				converge = job.getCounters().getGroup(Constants.COUNTER_GROUP)
						.findCounter(Constants.COUNTER_CONVERGED);
				total = job.getCounters().getGroup(Constants.COUNTER_GROUP)
						.findCounter(Constants.COUNTER_TOTAL);
				iterCounter++;
			}

		} catch (Exception e) {
			// TODO:
			// a better error report routine
			e.printStackTrace();
		}

	}
}
