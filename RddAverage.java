/**
 * 
 */
package TwitterRisingTrends;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
/**
 * @author 19230203
 *
 */
public class RddAverage implements VoidFunction<JavaRDD<Integer>>{

	// We needed to change to RDD because the the DStream count wasn't returning a long type for us to use
	// we the count to get the average
	@Override
	public void call(JavaRDD<Integer> rdd) throws Exception {
		long count = rdd.count();	// we need the total number of elements 
		
		if (count == 0)
			return;
		
		int total = rdd.reduce(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				
				return arg0+arg1;
			}
		});
		
		System.out.println(total/count); // log the correct average
		
	}

}
