package distanceMeasure;

import utils.VectorDoubleWritable;

public class EuclideanDistance implements DistanceMeasure {

	@Override
	public double distance(VectorDoubleWritable v1, VectorDoubleWritable v2,
			double lengthSquare) {
		return lengthSquare - 2 * v1.times(v2).sum() + v2.sumSquare();
	}

	@Override
	public double distance(VectorDoubleWritable v1, VectorDoubleWritable v2) {
		return this.distance(v1, v2, v1.sumSquare());
	}

}
