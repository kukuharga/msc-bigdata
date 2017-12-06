package uk.ac.qmul.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.qmul.exception.DataFilterException;
import uk.ac.qmul.type.Athlete;
import uk.ac.qmul.util.DataUtils;
import uk.ac.qmul.util.DataUtils.CustomCounters;
import uk.ac.qmul.writable.IntIntPair;

public class UserRatingMapper extends Mapper<Object, Text, IntWritable, IntIntPair> {
	private static Log log = LogFactory.getLog(UserRatingMapper.class);
	private static final String REGEX_DELIMITER = ";(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	private static final int TWEET = 2;
	private IntWritable userId = new IntWritable();
	private IntWritable movieId = new IntWritable();
	private IntWritable rating = new IntWritable();
	private IntIntPair movieRating = new IntIntPair();
	Map<Integer, String> movieNames = new HashMap<Integer, String>();
	Text data = new Text();

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntIntPair>.Context context)
			throws IOException, InterruptedException {

		try {

			String line = value.toString();
			String[] columns = line.split("\t");
			DataUtils.validateMovieRatingData(columns);
			userId.set(new Integer(columns[0]));
			movieId.set(new Integer(columns[1]));
			rating.set(new Integer(columns[2]));
			movieRating.set(movieId, rating);
			context.write(userId, movieRating);
		} catch (DataFilterException e) {
			context.getCounter(CustomCounters.INVALID_ROW).increment(1);
			log.warn(e);
		} catch (Exception e) {
			log.error("=====error====", e);
			throw e;
		}

	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {


			while ((line = br.readLine()) != null) {
				String[] fields = line.split("|");
				if (fields.length > 1 && NumberUtils.isDigits(fields[0]) && StringUtils.isNotEmpty(fields[1])) {
					movieNames.put(Integer.parseInt(fields[0]), fields[1]);
				}
			}
			br.close();
		} catch (IOException e1) {
			log.error(e1);
		}

		super.setup(context);
	}

}
