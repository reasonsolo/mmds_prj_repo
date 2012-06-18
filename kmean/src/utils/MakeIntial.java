package utils;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import vector.VectorDoubleWritable;
import clusterer.KmeansCluster;

public class MakeIntial {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		VectorDoubleWritable vec[] = new VectorDoubleWritable[10];
		KmeansCluster clusters[] = new KmeansCluster[10];
		try {
			String files[] = { "chess.txt", "door.txt", "fire.txt", "tank.txt",
					"television.txt", "world map.txt" };
			int i = 0;
			for (String file : files) {
				BufferedReader reader = new BufferedReader(new FileReader(
						"/home/phoenix/workspace/temp/" + file));
				String temp = null;
				temp = reader.readLine();
				vec[i++] = new VectorDoubleWritable(new Text(temp));
				reader.close();
			}

			Configuration conf = new Configuration();
			Path path = new Path("hdfs://master:54310/kmeans/initial/initial");
			FileSystem fs = FileSystem.get(new URI(
					"hdfs://master:54310/kmeans/initial/initial"), conf);

			SequenceFile.Writer writer = null;

			writer = new SequenceFile.Writer(fs, conf, path,
					LongWritable.class, KmeansCluster.class);

			for (i = 0; i < 6; i++) {
				clusters[i] = new KmeansCluster(i);
				clusters[i].addPoint(vec[i]);

				System.out.println(clusters[i].getId() + "\t"
						+ clusters[i].getCentroid());
				writer.append(new LongWritable(i), clusters[i]);
			}

			writer.syncFs();
			writer.sync();
			writer.close();
		} catch (IOException e) {
			assumeNoException(e);
			fail("Can't set up!");
		} catch (URISyntaxException e) {
			e.printStackTrace();
			fail("Can't set up!");
		}
	}
}
