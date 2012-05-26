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
		return v1.euclidianDistance(v2);
	}

}
