package tests;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import utils.VectorDoubleWritable;
import clusterer.Cluster;

public class ClusterTest {
	VectorDoubleWritable vec[] = new VectorDoubleWritable[10];

	@Before
	public void setUp() throws Exception {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					"dataset/testdata/test"));
			for (int i = 0; i < 10; i++) {
				vec[i] = new VectorDoubleWritable(new Text(reader.readLine()));
			}

			reader.close();
		} catch (IOException e) {
			Assume.assumeNoException(e);
		}
	}

	@Test
	public void testConstructor() {
		Cluster c = new Cluster();
		assertEquals(c.getSize(), 0);

		int i = 0;
		for (VectorDoubleWritable v : vec) {
			c.addPoint(v);
			i++;
			assertEquals(c.getSize(), i);
		}

		assertTrue(c.getCentroid().equals(vec[1].times(4.5)));
		assertTrue(c.getS1().equals(vec[9].times(5)));
		assertTrue(c.getS2().equals(vec[1].times(285)));
		assertFalse(c.getS2().equals(vec[1].times(287)));
		assertEquals(c.getSize(), 10);

		assertTrue(Math.abs(c.euclideanDistance(vec[1])
				- c.getCentroid().euclideanDistance(vec[1])) < 0.000000001);
	}

	@Test
	public void testOmitCluster() {
		
	}
}
