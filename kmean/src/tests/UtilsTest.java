package tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import vector.VectorDoubleWritable;

public class UtilsTest {
	VectorDoubleWritable vec[] = new VectorDoubleWritable[10];

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

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

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConstructor() {
		VectorDoubleWritable v1 = new VectorDoubleWritable(vec[4].get());
		v1.get().set(0, 0.0);
		assertTrue(Math.abs(vec[4].get().get(0) - 4) < 0.00000000001);
		assertTrue(Math.abs(v1.get().get(0) - 0) < 0.00000000001);

		v1.get().add(2.0);
		assertTrue(v1.size() == 11);
		assertTrue(vec[4].size() == 10);
	}

	@Test
	public void testAdd() {
		VectorDoubleWritable v1 = (VectorDoubleWritable) vec[1].clone();
		VectorDoubleWritable v2 = (VectorDoubleWritable) vec[2].clone();
		VectorDoubleWritable v3 = null;

		v3 = v1.plus(v2);
		assertTrue(v3.equals(vec[3]));

		v3 = v1.plus(4);
		assertTrue(v3.equals(vec[5]));

		assertEquals(v3.size(), vec[1].size());
		assertEquals(v3.size(), 10);

		v3 = v1.plus(2);
		assertTrue(v3.equals(vec[3]));
		assertEquals(v3.size(), 10);
	}

	@Test
	public void testTimes() {
		VectorDoubleWritable v1 = (VectorDoubleWritable) vec[2].clone();
		VectorDoubleWritable v2 = (VectorDoubleWritable) vec[4].clone();
		VectorDoubleWritable v3 = null;

		v3 = v1.times(v2);
		assertTrue(v3.equals(vec[8]));

		v3 = v1.times(3);
		assertTrue(v3.equals(vec[6]));
	}

	@Test
	public void testSumAndSqure() {
		assertTrue(Math.abs(vec[2].sum() - 2 * 10) < 0.000000000001);
		assertTrue(Math.abs(vec[2].sumSquare() - 4 * 10) < 0.000000000001);
	}

	@Test
	public void testClone() {
		VectorDoubleWritable v1 = (VectorDoubleWritable) vec[2].clone();
		v1.get().set(0, 0.0);
		assertTrue(Math.abs(vec[2].get().get(0) - 2) < 0.00000000001);
		assertTrue(Math.abs(v1.get().get(0) - 0) < 0.00000000001);

		v1.get().add(2.0);
		assertTrue(v1.size() == 11);
		assertTrue(vec[2].size() == 10);
	}
}
