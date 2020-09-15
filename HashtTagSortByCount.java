package TwitterRisingTrends;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
/**
 * @author 19230203
 * The Pair is hashtag and its count, lets sort by count
 */
public class HashtTagSortByCount implements Function<JavaPairRDD<Long,String>, JavaPairRDD<Long, String>> {
	

	@Override
	public JavaPairRDD<Long, String> call(JavaPairRDD<Long, String> pairRDD) throws Exception {
		return pairRDD.sortByKey(false); 
	}

}
