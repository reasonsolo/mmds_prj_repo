package tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import utils.VectorDoubleWritable;
import clusterer.KmeansCluster;

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
		KmeansCluster c = new KmeansCluster(0);
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
		KmeansCluster c1 = new KmeansCluster(0), c2 = new KmeansCluster(1);
		assertEquals(c1.getSize(), 0);

		int i = 0;
		for (VectorDoubleWritable v : vec) {

			i++;
			if (i % 2 == 1)
				c1.addPoint(v);
			if (i % 2 == 0)
				c2.addPoint(v);
			assertEquals(c1.getSize(), (i + 1) / 2);
			assertEquals(c2.getSize(), i / 2);
		}

		c1.omitCluster(c2);

		assertTrue(c1.getCentroid().equals(vec[1].times(4.5)));
		assertTrue(c1.getS1().equals(vec[9].times(5)));
		assertTrue(c1.getS2().equals(vec[1].times(285)));
		assertFalse(c1.getS2().equals(vec[1].times(287)));
		assertEquals(c1.getSize(), 10);

		assertTrue(Math.abs(c1.euclideanDistance(vec[1])
				- c1.getCentroid().euclideanDistance(vec[1])) < 0.000000001);
	}
}
