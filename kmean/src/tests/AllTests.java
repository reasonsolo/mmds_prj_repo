package tests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ ClustererTest.class, ClusterTest.class,
		DistanceMeasureTest.class, UtilsTest.class })
public class AllTests {

}
