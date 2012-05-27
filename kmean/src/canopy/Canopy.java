package canopy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ListIterator;

import org.apache.hadoop.io.Writable;

import utils.VectorDoubleWritable;

public class Canopy implements Writable {
	protected Integer id;
	protected Integer size;
	protected VectorDoubleWritable center;
	protected VectorDoubleWritable s1;
	protected VectorDoubleWritable s2;
	protected VectorDoubleWritable weight;

	public Canopy() {
		s1 = new VectorDoubleWritable();
		s2 = new VectorDoubleWritable();
		weight = new VectorDoubleWritable();
		center = new VectorDoubleWritable();
		size = 0;

	}

	public Canopy(int i, VectorDoubleWritable c) {
		id = i;
		s1 = new VectorDoubleWritable();
		s2 = new VectorDoubleWritable();
		weight = new VectorDoubleWritable();
		center = c;
		size = 1;
	}

	public void addPoint(VectorDoubleWritable point)
			throws IllegalStateException {
		if (s1.size() != point.size())
			throw new IllegalStateException("Dimension mismatch!");
		ListIterator<Double> ite1 = point.get().listIterator();
		ListIterator<Double> ite2 = s1.get().listIterator();
		ListIterator<Double> ite3 = s2.get().listIterator();
		Double p = 0.0;
		Double c = 0.0;
		Double s = 0.0;
		for (; ite1.hasNext();) {
			p = ite1.next();
			c = ite2.next();
			s = ite3.next();
			ite2.set(c + p);
			ite3.set(s + p * p);
		}
		size++;
	}

	public double euclideanDistance(VectorDoubleWritable point)
			throws IllegalStateException {
		return point.euclideanDistance(this.center);
	}

	@Override
	// read and write
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		size = in.readInt();
		center.readFields(in);
		s1.readFields(in);
		s2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeDouble(size);
		center.write(out);
		s1.write(out);
		s2.write(out);
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public VectorDoubleWritable getS1() {
		return s1;
	}

	public void setS1(VectorDoubleWritable s1) {
		this.s1 = s1;
	}

	public VectorDoubleWritable getS2() {
		return s2;
	}

	public void setS2(VectorDoubleWritable s2) {
		this.s2 = s2;
	}

	public Integer getSize() {
		return size;
	}

	public void setSize(Integer size) {
		this.size = size;
	}

	public VectorDoubleWritable getCentroid() {
		VectorDoubleWritable centroid = (VectorDoubleWritable) s1.clone();
		ListIterator<Double> ite = centroid.get().listIterator();
		double data = 0;
		while (ite.hasNext()) {
			data = ite.next();
			ite.set(data / size);
		}
		return centroid;
	}

	public double variance() {
		double s1avg = s1.sum() / size;
		return s2.sum() / size - s1avg * s1avg;
	}

	public double standardDiviation() {
		return Math.sqrt(variance());
	}

	public VectorDoubleWritable getCenter() {
		return center;
	}
}
