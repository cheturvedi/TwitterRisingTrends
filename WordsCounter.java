package TwitterRisingTrends;

import org.apache.spark.api.java.function.Function;

/**
 * @author 19230203
 *
 */
public class WordsCounter implements Function<String, Integer>{

	/**
	 * Count the words
	 */
	private static final long serialVersionUID = 19230203L;
	
	@Override
	public Integer call(String tweet) throws Exception {
		
		return new Integer(tweet.split(" ").length); // splitting on a whitespace is how we get number of words
	}

}
