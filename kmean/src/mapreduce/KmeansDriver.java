package mapreduce;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import utils.VectorDoubleWritable;
import clusterer.Cluster;
import clusterer.Clusterer;
import config.ConfigConstants;
import distanceMeasure.DistanceMeasure;
import distanceMeasure.EuclideanDistance;


public class KmeansDriver {
    public static void configure() {

    }
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: " + args[0] + "<input> <output>");
            System.exit(0);
        }

        int NumberOfClusters = Int(args[3]);
        Configuration conf = new Configuration();
		conf.setNumReduceTasks(10);
        Path in = new Path(args[1]);
        Path out = new Path(args[2]);

        int coverge = 0;
        int total = 0;
        try {
            
            while (converge != total) {
                Job job = new Job(conf);
                job.setJobName("K-means clustering");

                job.setMapperClass(KmeansMapper.class);
                job.setReducerClass(KmeansReducer.class);
                job.setJarByClass(KmeansDriver.class);

                SequenceFileInputFormat.addInputPath(job, in);
                SequenceFileOutputFormat.addOutputPath(job, out);
                job.setInputFormatClass(SequenceFileInputFormat.class);
                job.setOutputFormatClass(SequenceFileOutputFormat.class);

                job.setOutputKeyClass(IntWritable);
                job.setOutputValueClass(Cluster);

                job.waitForCompletion(true);

                converge = job.getCounters("Clusterer", "ConvergedCluster");
                total = job.getCounters("Clusterer", "TotalCluster");
            }
            
        } catch (Exception e) {
            // TODO:
            // a better error report routine
            e.printStackTrace();
        }
        
    }
}
