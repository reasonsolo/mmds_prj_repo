import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class NetflixClustererDriver {

	// This map-reduce selects the canopies to be used for clustering.
	// Each mapper performs the canopy selection algorithm over the data that is is presented.
	// Each mapper then emits what it thinks should be canopy centers from its data set.
	// The single reducer then amalagates those canopy centers into a single set of centers, performing
	// the same selection algorithm.
	
	// This probably does not need to be a map-reduce job, especially as it is only run once.
	
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(NetflixClustererDriver.class);
		conf.setJobName("NetflixCanopyMaker");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

        conf.setNumReduceTasks(1);
		
		conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);		
		conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class);

        if (args.length < 2) {
            System.out.println("Usage: NetflixCanopyMaker <input path> <output path>");
            System.exit(0);
         }
         conf.setInputPath(new Path(args[0]));
         conf.setOutputPath(new Path(args[1]));

		conf.setMapperClass(NetflixCanopySelector.class);
		conf.setReducerClass(NetflixCanopyAmalagator.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
