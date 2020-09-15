/**
 * 
 */
package TwitterRisingTrends;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * @author 19230203
 * Each tweet will hold a list of tags, we will un-nest this into one list for all tweets using flatmap
 */
public class HashTagExtracter implements FlatMapFunction<String, String>{

	/**
	 * Using regex lets extract the hashtag
	 PS : only works on latin based languages where script is written from left to right
	  does not work on arabic
	 */
	private static final long serialVersionUID = 19230203L;
	
	@Override
	public Iterator<String> call(String tweet) throws Exception {
		List<String> tags = new ArrayList<String>();
		Pattern pattern = Pattern.compile("#(\\w+)", //starts with hashtag , match the whole word
				Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CHARACTER_CLASS); // all tweets are in unicode
		Matcher matcher = pattern.matcher(tweet);

		while (matcher.find()) {
			tags.add(matcher.group());
		}

		return tags.iterator();
	}

}
