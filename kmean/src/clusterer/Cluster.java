package clusterer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ListIterator;

import org.apache.hadoop.io.Writable;

import utils.VectorDoubleWritable;

public class Cluster implements Writable {
	protected int id;
	protected int size;
	protected VectorDoubleWritable s1;
	protected VectorDoubleWritable s2;

	public Cluster() {
		s1 = new VectorDoubleWritable();
		s2 = new VectorDoubleWritable();
		id = 0;
		size = 0;
	}

	public Cluster(int id, VectorDoubleWritable s1, VectorDoubleWritable s2,
			int size) {
		super();
		this.s1 = s1;
		this.s2 = s2;
		this.size = size;
		this.id = id;
	}

	public void addPoint(VectorDoubleWritable point)
			throws IllegalStateException {
		if (s1.size() != point.size())
			throw new IllegalStateException("Dimension doesn't agree!");
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
			ite1.set(c + p);
			ite3.set(s + p);
		}
		size++;
	}

	public double distance(VectorDoubleWritable point)
			throws IllegalStateException {
		return point.euclidianDistance(this.getCentroid());
	}

	public void omitCluster(Cluster omitee) {
		s1 = s1.plus(omitee.getS1());
		s2 = s2.plus(omitee.getS2());
		size += omitee.getSize();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		size = in.readInt();
		s1.readFields(in);
		s2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeDouble(size);
		s1.write(out);
		s2.write(out);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
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

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
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
}
