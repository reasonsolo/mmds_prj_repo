package tests;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import vector.VectorDoubleWritable;
import distanceMeasure.DistanceMeasure;
import distanceMeasure.EuclideanDistance;

public class DistanceMeasureTest {
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
			fail("Can't read file dataset/testdata/test");
		}
	}

	@Test
	public void testEuclieanDistance() {
		DistanceMeasure dm = new EuclideanDistance();

		assertTrue(dm.distance(vec[1], vec[3]) == 40);
		assertTrue(dm.distance(vec[1], vec[3]) == dm.distance(vec[1], vec[3],
				vec[1].sumSquare()));

		assertTrue(dm.distance(vec[3], vec[9]) == 360);
		assertTrue(dm.distance(vec[3], vec[9]) == dm.distance(vec[9], vec[3],
				vec[9].sumSquare()));
	}

}
