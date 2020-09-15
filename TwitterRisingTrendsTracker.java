package TwitterRisingTrends;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

/**
 * @author 19230203
 *
 */
public class TwitterRisingTrendsTracker {

	public static void main(String[] args) {

		if (args.length == 0) {
			System.out.println("Please specify the mode to run as command line argument.\n Options are [1/2/3]");
			System.exit(0);
		}

		int option = Integer.parseInt(args[0]);

		// this turns off the excess logging from spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// If HADOOP_HOME is not set
		System.setProperty("hadoop.home.dir", "C:/winutils");
		// Configure spark with number of cores and memory size
		SparkConf sparkConf = new SparkConf().setAppName("TwitterStream").setMaster("local[4]")
				.set("spark.executor.memory", "1g");

		// Set up spark context to create RDDs
		// This refreshes the stream every second`
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

		
		// Twitter API requires these 4 pieces to authenticate and pull tweets using the internet
		// API keys are removed hidden for security
		final String consumerKey = ""; 
		final String consumerKeySecret = "";
		final String accessToken = "";
		final String accessTokenSecret = "";

		//since we didnt use a file to save our key, lets put the twitter keys to system property
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerKeySecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);



		// All the helper functions. each class documents its own purpose
		HashtTagSortByCount sorter = new HashtTagSortByCount();
		ReduceStreamToAvg mean = new ReduceStreamToAvg();
		CharactersCounter charCounter = new CharactersCounter();
		WordsCounter wordsCounter = new WordsCounter();
		HashTagExtracter tagExtractor = new HashTagExtracter();
		TweetExtractor tweetExtractor = new TweetExtractor();

		// Pull tweets 
		JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(ssc);
		
		// Extract the text of the tweets from the tweet objects
		JavaDStream<String> statuses = tweets.map(tweetExtractor);
		
		// Tally number of characters from each extracted tweet
		JavaDStream<Integer> numCharacters = statuses.map(charCounter);
		
		// Tally number of words from each extracted tweet
		JavaDStream<Integer> numWords = statuses.map(wordsCounter);
		
		// Tally all the hastags per tweet based on regex
		JavaDStream<String> hashtags = statuses.flatMap(tagExtractor);
		// aggregtor helper class to compute averages for number of characters and words
		RddAverage averageAggregator = new RddAverage();
		
		// Print live tweets
		if (option == 1) {
			statuses.print(); // Prints 10 tweets
		}
		
		// Extracting character count, word count and hashtags from live tweets	
		else if (option == 2) {
			statuses.mapToPair(new PairFunction<String, String, Integer>() {

				@Override
				public Tuple2<String, Integer> call(String tweet) throws Exception {
					return new Tuple2<String, Integer>(tweet, tweet.length()); // Prints (tweet, num of characters)
				}

			}).print(); 

			statuses.mapToPair(new PairFunction<String, String, Integer>() {

				@Override
				public Tuple2<String, Integer> call(String tweet) throws Exception {
					return new Tuple2<String, Integer>(tweet, tweet.split(" ").length); // Prints (tweet, num of words)
				}

			}).print();

			statuses.mapToPair(new PairFunction<String, String, List<String>>() {

				@Override
				public Tuple2<String, List<String>> call(String tweet) throws Exception {
					List<String> tags = new ArrayList<String>();
					Pattern pattern = Pattern.compile("#(\\w+)",
							Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CHARACTER_CLASS);
					Matcher matcher = pattern.matcher(tweet);

					while (matcher.find()) {
						tags.add(matcher.group());
					}
					return new Tuple2<String, List<String>>(tweet, tags); // Prints (tweet, [List of hastags])
				}

			}).print();
		}
		// Counts the frqeuencies of hashtags in live tweets
		else if (option == 3) {

			
			// The stream refreshes every second
			// The RDD in the stream is used to compute the averages each time there is a refreshes
			// As a result we are getting averges continuesly
			numCharacters.foreachRDD(averageAggregator);
			numWords.foreachRDD(averageAggregator);
			
			hashtags.countByValue() // create tuple of (hashtag , count)
			.mapToPair(x -> x.swap()) // create tuple of (count,hashtag)
			.transformToPair(sorter) // sort by count
			.print();


		// Count the frequencies of all hashtags every 30 seconds
		// In this stream, we can track rising and falling hashtags 
		} else {



			// repeat the above calculation in 5 min window , every 30 seconds 
			numCharacters.window(Durations.minutes(5), Durations.seconds(30)).foreachRDD(averageAggregator);

			numWords.window(Durations.minutes(5), Durations.seconds(30)).foreachRDD(averageAggregator);

			
			hashtags.window(Durations.minutes(5), Durations.seconds(30)).countByValue().mapToPair(x -> x.swap())
					.transformToPair(sorter).print();

		}

		
		// start processing
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
