package uk.ac.qmul.mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.qmul.exception.DataFilterException;
import uk.ac.qmul.util.DataUtils;
import uk.ac.qmul.util.DataUtils.CustomCounters;
import uk.ac.qmul.writable.IntIntPair;

public class TweetHashTagMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static Log log = LogFactory.getLog(TweetHashTagMapper.class);
	private static final String REGEX_DELIMITER = ";(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	private static final int EPOCH_TIME = 0;
	private static final int TWEET = 2;
	private int mostPopularHour = 1;
	private final IntWritable one = new IntWritable(1);
	private Text data = new Text();

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		try {
			
			String line = value.toString();
			String[] columns = line.split(REGEX_DELIMITER);
			DataUtils.validateData(columns);
			//Check if tweet message was posted during most popular hour
			if (this.mostPopularHour == DataUtils.getTweetHour(columns[EPOCH_TIME])) {
				//Extract hashtags from the tweet
				Set<String> hashTagList = DataUtils.getHashtagList(columns[TWEET]);
				for (String hashTag : hashTagList) {
					data.set(hashTag);
					context.write(data, one);
				}
			}

		} catch (DataFilterException e) {
			context.getCounter(CustomCounters.INVALID_ROW).increment(1);
			log.warn(e);
		} catch (Exception e) {
			throw e;
		}

	}

	@Override
	protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		this.mostPopularHour =  context.getConfiguration().getInt(DataUtils.BUSIEST_HOUR,DataUtils.MOST_POPULAR_HOUR);	
		log.info("Most Popular Hour ----> "+this.mostPopularHour);
		super.setup(context);
	}
	
	

}
