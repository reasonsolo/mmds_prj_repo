package tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import utils.VectorDoubleWritable;
import clusterer.KmeansCluster;
import clusterer.KmeansClusterer;

public class ClustererTest {
	KmeansClusterer clusterer = new KmeansClusterer();
	VectorDoubleWritable vec[] = new VectorDoubleWritable[20];

	@Before
	public void setUp() {
		KmeansCluster clusters[] = new KmeansCluster[5];
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					"dataset/testdata/cluster/testcluster"));
			String temp = null;
			for (int i = 0; i < 14; i++) {
				temp = reader.readLine();
				vec[i] = new VectorDoubleWritable(new Text(temp));
			}
			reader.close();

			Configuration conf = new Configuration();
			Path path = new Path("hdfs://master:54310/kmeans/initial/initial");
			FileSystem fs = FileSystem.get(new URI(
					"hdfs://master:54310/kmeans/initial/initial"), conf);

			SequenceFile.Writer writer = null;

			writer = new SequenceFile.Writer(fs, conf, path,
					LongWritable.class, KmeansCluster.class);

			for (int i = 0; i < 3; i++) {
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

	@Test
	public void testLoadClusters() throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		String clusterPath = "hdfs://master:54310/kmeans/initial/initial";

		ArrayList<KmeansCluster> clusters = clusterer.getClusters();
		assertEquals(clusters.size(), 0);

		clusterer.loadClusters(clusterPath, conf);

		int i = 0;
		for (KmeansCluster cluster : clusters) {
			assertTrue(cluster.getId() == i);
			assertTrue(clusterer.getClusters().contains(cluster));
			assertEquals(cluster.getCentroid().size(), 2);
			i++;
		}

		assertEquals(clusters.size(), 3);
	}
}
