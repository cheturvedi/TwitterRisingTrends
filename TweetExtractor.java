package TwitterRisingTrends;

import org.apache.spark.api.java.function.Function;

import twitter4j.Status;

/**
 * @author 19230203
 *
 */
public class TweetExtractor implements Function<Status, String>{
	
	/**
	 * Get the tweet contents from status objects
	 */
	private static final long serialVersionUID = 19230203L;

	@Override
	public String call(Status status) throws Exception {
		return status.getText();
	}

}
