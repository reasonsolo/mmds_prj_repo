package clusterer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ListIterator;

import org.apache.hadoop.io.Writable;

import utils.VectorDoubleWritable;

public class Cluster implements Writable {
	protected int id;
	protected VectorDoubleWritable centroid;
	protected VectorDoubleWritable s2;
	protected int size;

	public Cluster() {
		centroid = new VectorDoubleWritable();
		id = 0;
		size = 0;
	}

	public Cluster(int id, VectorDoubleWritable centroid,
			VectorDoubleWritable s2, int size) {
		super();
		this.centroid = centroid;
		this.s2 = s2;
		this.size = size;
		this.id = id;
	}

	public void addPoint(VectorDoubleWritable point)
			throws IllegalStateException {
		if (centroid.size() != point.size())
			throw new IllegalStateException("Dimension doesn't agree!");
		ListIterator<Double> ite1 = point.get().listIterator();
		ListIterator<Double> ite2 = centroid.get().listIterator();
		ListIterator<Double> ite3 = s2.get().listIterator();
		Double p = 0.0;
		Double c = 0.0;
		Double s = 0.0;
		for (; ite1.hasNext();) {
			p = ite1.next();
			c = ite2.next();
			s = ite3.next();
			ite1.set((c * size + p) / (size + 1));
			ite3.set(s + p);
		}
	}

	public double distance(VectorDoubleWritable point)
			throws IllegalStateException {
		return point.euclidianDistance(centroid);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		size = in.readInt();
		centroid.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeDouble(size);
		centroid.write(out);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public VectorDoubleWritable getCentroid() {
		return centroid;
	}

	public void setCentroid(VectorDoubleWritable centroid) {
		this.centroid = centroid;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
}
