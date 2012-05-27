package clusterer;

import utils.VectorDoubleWritable;

public class KmeansCluster extends Cluster implements Cloneable {

	public KmeansCluster() {
		super();
	}

	public KmeansCluster(int i) {
		super(i);
	}

	public KmeansCluster(int id, VectorDoubleWritable s1,
			VectorDoubleWritable s2) throws IllegalStateException {
		super(id, s1, s2);
	}
}
