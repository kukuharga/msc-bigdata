package uk.ac.qmul.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
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

public class TweetHistogramMapper extends Mapper<Object, Text, IntIntPair, IntWritable> {
	private static Log log = LogFactory.getLog(TweetHistogramMapper.class);
	private static final String REGEX_DELIMITER = ";(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	private static final int EPOCH_TIME = 0;
	private static final int TWEET_ID = 1;
	private static final int TWEET = 2;
	private static final int DEVICE = 3;
	private final IntWritable one = new IntWritable(1);

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntIntPair, IntWritable>.Context context)
			throws IOException, InterruptedException {

		try {
			String line = value.toString();
			// Split input into data array
			String[] columns = line.split(REGEX_DELIMITER);
			// Validate data array
			DataUtils.validateData(columns);
			// Get the correct histogram bin for the tweet length using method getIntIntHistogramRange, and
			// write key and value to the output
			context.write(DataUtils.getIntIntHistogramRange(columns[TWEET]), one);
		} catch (DataFilterException e) {
			//Count invalid data
			context.getCounter(CustomCounters.INVALID_ROW).increment(1);
			log.warn(e);
		} catch (Exception e) {
			throw e;
		}

	}

}
