package uk.ac.qmul.job;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import uk.ac.qmul.mapper.TweetHashTagMapper;
import uk.ac.qmul.reducer.TweetHashTagReducer;
import uk.ac.qmul.util.DataUtils;

public class TweetHashTagJob {
	
	public static void runJob(String input, String output, int busiestHour) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		job.setJarByClass(TweetHashTagJob.class);
		job.setMapperClass(TweetHashTagMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(TweetHashTagReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(2);
		job.getConfiguration().setInt(DataUtils.BUSIEST_HOUR, busiestHour);
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		switch (args.length) {
		case 2:
			runJob(args[0],args[1],DataUtils.MOST_POPULAR_HOUR);
			break;
			
		case 3:
			if(!NumberUtils.isNumber(args[2]) && (Integer.parseInt(args[2]) >= 0 && Integer.parseInt(args[2]) < 24)) 
				throw new Exception("Last argument is not a number between 0-23");
			runJob(args[0], args[1],Integer.parseInt(args[2]));
			break;	

		default:
			System.out.println("Invalid arguments to run this job. Correct arguments : <input> <output> <busiestHour (optional)>.");
			System.exit(1);
			break;
		}
		
		
	}

}
