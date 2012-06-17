package distanceMeasure;

import vector.VectorDoubleWritable;

public interface DistanceMeasure {

	public double distance(VectorDoubleWritable v1, VectorDoubleWritable v2, double SquareS2);
	public double distance(VectorDoubleWritable v1, VectorDoubleWritable v2);
}
