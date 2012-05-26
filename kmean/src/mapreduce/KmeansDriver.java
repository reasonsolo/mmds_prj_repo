import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class KmeansDriver {
    public static void main(String[] args) {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(KmeansIterDriver.class);
        conf.setJobName("K-Means Iteration Driver");

        // FIXME
        // use correct output classes combination 
        // maybe VectorDoubleWritable.class
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        // use a proper tasks configuration
        // how to specify the method of converting lines in a file to objects 
        conf.setNumReduceTasks(10);
        conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
        conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class);
        if (args.length < 3) {
            // FIXME:
            // print a better instruction helper
            System.out.println("Usage: " + args[0] + "<input> <output>");
            System.exit(0);
        }

        conf.setInputPath(new Path(args[1]));
        conf.setOutputPath(new Path(args[2]));

        conf.setMapperClass(KmeansMapper.class);
        conf.setReducerClass(KmeansReducer.class);

        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            // TODO:
            // a better error report routine
            e.printStackTrace();
        }
        
    }
}
