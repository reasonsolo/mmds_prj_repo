package clusterer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ListIterator;

import vector.VectorDoubleWritable;

public class CanopyCluster extends Cluster {
	protected VectorDoubleWritable center;
	protected VectorDoubleWritable weight;

	public CanopyCluster() {
		super();
		weight = new VectorDoubleWritable();
		center = new VectorDoubleWritable();
	}

	public CanopyCluster(int i, VectorDoubleWritable c) {
		super(1, c, c.times(c));

		weight = new VectorDoubleWritable();
		center = c;
	}

	public void addPoint(VectorDoubleWritable point)
			throws IllegalStateException {
		if (size == 0) {
			s1 = (VectorDoubleWritable) point.clone();
			s2 = point.times(point);
			System.out.println(s1.size());
			System.out.println(point.size());
			for (Double elem : point.get()) {
				System.out.println(elem.toString());
			}
		} else {
			if (s1.size() != point.size()) {
				System.out.println(s1.size());
				System.out.println(point.size());
				for (Double elem : point.get()) {
					System.out.println(elem.toString());
				}
				throw new IllegalStateException("Dimension mismatch!");
			}
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
			System.out.println(s1.size());
			System.out.println(s2.size());

		}
		size++;
	}

	@Override
	// read and write
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		center.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		center.write(out);
	}

	public VectorDoubleWritable getCenter() {
		return center;
	}
}