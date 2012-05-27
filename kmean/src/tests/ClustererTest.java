package tests;

import static org.junit.Assert.*;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import utils.VectorDoubleWritable;
import clusterer.Cluster;
import clusterer.Clusterer;

public class ClustererTest {
	Clusterer clusterer = new Clusterer();
	Cluster clusters[] = new Cluster[5];
	VectorDoubleWritable vec[] = new VectorDoubleWritable[20];

	@Before
	public void setUp() {
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

			writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class,
					Cluster.class);

			for (int i = 0; i < 3; i++) {
				clusters[i] = new Cluster(i);
				clusters[i].addPoint(vec[i]);

				writer.append(new IntWritable(i), clusters[i]);
				System.out.println(clusters[i].getCentroid());
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

		ArrayList<Cluster> clusters = clusterer.getClusters();
		assertEquals(clusters.size(), 0);

		clusterer.loadClusters(clusterPath, conf);

		for (Cluster cluster : clusters) {
			System.out.println(cluster.getCentroid());
		}

		assertEquals(clusters.size(), 3);
	}
}
