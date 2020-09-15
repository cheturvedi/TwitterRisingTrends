package TwitterRisingTrends;

import org.apache.spark.api.java.function.Function;

/**
 * @author 19230203
 *
 */
public class CharactersCounter implements Function<String, Integer>{

	/**
	 * The twitter text is a string , so getting the character length is enough 
	 */
	private static final long serialVersionUID = 19230203L;

	@Override
	public Integer call(String tweet) throws Exception {
		return new Integer(tweet.length());
	}

}
