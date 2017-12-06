package uk.ac.qmul.job;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import uk.ac.qmul.mapper.TweetAthleteMentionsMapper2;
import uk.ac.qmul.reducer.TweetAthleteMentionsReducer;



public class TweetAthleteMentionsJob2 {
	private static Log log = LogFactory.getLog(TweetAthleteMentionsJob2.class);
	private static final String ADDITIONAL_INPUT = "EXTRA_INPUT";

	public static void runJob(String input1, String input2,String output) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance();
		job.setJarByClass(TweetAthleteMentionsJob2.class);
		job.setMapperClass(TweetAthleteMentionsMapper2.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(TweetAthleteMentionsReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(3);
		
	

		job.addCacheFile(new Path(input2).toUri());
		
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job,input1);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		
		switch (args.length) {
		case 2:
			runJob(args[0], "/data/medalistsrio.csv",args[1]);
			break;
			
		case 3:
			runJob(args[0], args[1],args[2]);
			break;	

		default:
			System.out.println("Not enough argument to run this job");
			System.exit(1);
			break;
		}
		

		
	}

}
