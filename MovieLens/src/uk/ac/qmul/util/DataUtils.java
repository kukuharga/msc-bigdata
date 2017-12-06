package uk.ac.qmul.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import uk.ac.qmul.exception.DataFilterException;
import uk.ac.qmul.writable.IntIntPair;

public class DataUtils {

	private static final int COL_SIZE = 4;
	private static final String DELIMITER = ";";
	private static final String REGEX_DELIMITER = ";(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	private static final int USER_ID = 0;
	private static final int MOVIE_ID = 1;
	private static final int RATING = 2;
	private static final int DEVICE = 3;
	private static final int EPOCH_TIME = 0;
	private static final int TWEET_ID = 1;
	private static final int TWEET = 2;
	// Max Length of Tweet that will be processed
	private static final int MAX_TWEET_LENGTH = 140;
	// Interval for Histogram
	private static final int HISTOGRAM_RANGE = 5;

	public enum CustomCounters {
		NUM_ATHLETES, INVALID_ROW
	}

	public enum CustomConfig {
		BUSIEST_HOUR
	}

	public static final String BUSIEST_HOUR = "BUSIEST_HOUR";
	public static final int MOST_POPULAR_HOUR = 1;

	public static void validateMovieRatingData(String[] columns) throws DataFilterException {
		// validate column count
		if (columns.length > 3)
			throw new DataFilterException("Following line doesn't have minimum 3 column data : "
					+ Arrays.toString(columns) + "\n.Line ignored.");

		// Validate user ID string
		if (NumberUtils.isNumber(columns[USER_ID]) == false)
			throw new DataFilterException(
					"Following line doesn't have valid user id : " + columns[USER_ID] + "\n.Line ignored.");

		// Validate movie ID string
		if (NumberUtils.isNumber(columns[MOVIE_ID]) == false)
			throw new DataFilterException(
					"Following line doesn't have valid movie id : " + columns[MOVIE_ID] + "\n.Line ignored.");

		// Validate rating string
		if (NumberUtils.isNumber(columns[RATING]) == false)
			throw new DataFilterException(
					"Following line doesn't have valid rating : " + columns[RATING] + "\n.Line ignored.");

	}

	public static void validateData(String[] columns) throws DataFilterException {
		// validate column count
		if (columns.length != COL_SIZE)
			throw new DataFilterException(
					"Following line doesn't have 4 column data : " + Arrays.toString(columns) + "\n.Line ignored.");
		// validate for any empty tweet
		if (StringUtils.isEmpty(columns[TWEET]))
			throw new DataFilterException("Empty Tweet for Tweet ID : " + columns[TWEET_ID] + ".Line ignored.");
		// validate for maximum tweet length
		if (columns[TWEET].length() > MAX_TWEET_LENGTH)
			throw new DataFilterException("Tweet length exceeds " + MAX_TWEET_LENGTH + " for Tweet ID : "
					+ columns[TWEET_ID] + ".Line ignored.");
		// Validate epoch time string
		if (NumberUtils.isNumber(columns[EPOCH_TIME]) == false)
			throw new DataFilterException(
					"Following line doesn't have valid epoch time : " + columns[EPOCH_TIME] + "\n.Line ignored.");

	}

	public static int getTweetHour(String tweetEpochTime) {
		long epochTime = Long.parseLong(tweetEpochTime);
		LocalDateTime time = LocalDateTime.ofEpochSecond(epochTime / 1000, 0, ZoneOffset.UTC);
		return time.getHour();
	}

	/**
	 * Validate string to contain ONLY letter and number.
	 * 
	 * @param line
	 *            String to validate
	 * @return true if string valid
	 */
	private static boolean validateHashTag(String line) {
		if ("".equals(line.trim()))
			return false;
		return Pattern.matches("[a-zA-Z0-9]*", line);
	}

	/**
	 * Remove sharp '#' symbol at the beginning of a string
	 * 
	 * @param x
	 *            String input
	 * @return Modified string without '#' symbol at the beginning
	 */
	private static String removeSharp(String x) {
		return (x.indexOf("#") != -1 && x.lastIndexOf("#") < x.length()) ? x.substring(x.lastIndexOf("#") + 1) : x;
	}

	/**
	 * Get hashtaglist from a String
	 * 
	 * @param tweet
	 *            String input of a twitter message
	 * @return List of hashtag string contained in the String input
	 */
	public static Set<String> getHashtagList(String tweet) {
		Pattern MY_PATTERN = Pattern.compile("#(\\S+)");
		Matcher mat = MY_PATTERN.matcher(tweet);
		Set<String> strs = new HashSet<String>();
		while (mat.find()) {
			String hashTag = removeSharp(mat.group(1));
			if (validateHashTag(hashTag))
				strs.add(hashTag);
		}
		return strs;
	}

	public static String[] splitStringByWhiteSpace(String input) {
		return (StringUtils.isNotEmpty(input)) ? input.split("\\s+") : null;
	}

	public static int getHistogramRange(String tweet) {
		int tweetLength = tweet.length();
		return Double.valueOf(Math.ceil((float) tweetLength / HISTOGRAM_RANGE)).intValue();
	}

	public static IntIntPair getIntIntHistogramRange(String tweet) {
		int tweetLength = tweet.length();
		int hourRange = Double.valueOf(Math.ceil((float) tweetLength / HISTOGRAM_RANGE)).intValue();
		int finish = hourRange * HISTOGRAM_RANGE;
		int start = (finish - HISTOGRAM_RANGE) + 1;
		return new IntIntPair(start, finish);
	}
}
