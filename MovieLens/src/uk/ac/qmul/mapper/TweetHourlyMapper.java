package uk.ac.qmul.mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.StringTokenizer;
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

public class TweetHourlyMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	private static Log log = LogFactory.getLog(TweetHourlyMapper.class);
	private static final int COL_SIZE = 4;
	private static final String DELIMITER = ";";
	private static final String REGEX_DELIMITER = ";(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	private static final int EPOCH_TIME = 0;
	private static final int TWEET_ID = 1;
	private static final int TWEET = 2;
	private static final int DEVICE = 3;
	// Max Length of Tweet that will be processed
	private static final int MAX_TWEET_LENGTH = 140; 
	// Interval for Histogram
	private static final int HISTOGRAM_RANGE = 5; 
	private final IntWritable one = new IntWritable(1);
	private IntWritable data = new IntWritable();
	

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {

		try {
			
			String line = value.toString();
			// Split input into data array
			String[] columns = line.split(REGEX_DELIMITER);
			// Validate data array
			DataUtils.validateData(columns);
			// Get the hour of tweet message using method getTweetHour, and
			// write key and value to the output
			data.set(DataUtils.getTweetHour(columns[EPOCH_TIME]));
			context.write(data, one);
		} catch (DataFilterException e) {
			//Count invalid data
			context.getCounter(CustomCounters.INVALID_ROW).increment(1);
			log.warn(e);
		} catch (Exception e) {
			throw e;
		}

	}


	

	
	
	
	
	

}
