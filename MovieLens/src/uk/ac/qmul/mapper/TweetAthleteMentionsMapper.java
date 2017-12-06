package uk.ac.qmul.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.qmul.exception.DataFilterException;
import uk.ac.qmul.util.DataUtils;
import uk.ac.qmul.util.DataUtils.CustomCounters;

public class TweetAthleteMentionsMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static Log log = LogFactory.getLog(TweetAthleteMentionsMapper.class);
	private static final String REGEX_DELIMITER = ";(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	private List<String> athleteList;
	private final IntWritable one = new IntWritable(1);
	private static final int TWEET = 2;
	private Text data = new Text();
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		try {

			String line = value.toString();
			String[] columns = line.split(REGEX_DELIMITER);
			DataUtils.validateData(columns);

			for (String athleteName : athleteList) {
				if (columns[TWEET].toLowerCase().contains(athleteName.toLowerCase())) {
					data.set(athleteName);
					context.write(data, one);
				}

			}

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

		athleteList = new ArrayList<String>();

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				String[] fields = line.split(",");

				if (fields.length == 11 && StringUtils.isNotEmpty(fields[1])) {
					athleteList.add(fields[1]);
					context.getCounter(CustomCounters.NUM_ATHLETES).increment(1);
				}
			}
			br.close();
		} catch (IOException e1) {
			log.error(e1);
		}catch (Exception e2) {
			log.error(e2);
		}

		super.setup(context);
	}

}
