package uk.ac.qmul.job;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import uk.ac.qmul.mapper.TweetHistogramMapper;
import uk.ac.qmul.reducer.IntSumReducer;
import uk.ac.qmul.reducer.TweetHistogramReducer;
import uk.ac.qmul.writable.IntIntPair;

public class TweetHistogramJob {

  public static void runJob(String[] input, String output) throws Exception {

        Configuration conf = new Configuration();

    Job job = Job.getInstance();
    job.setJarByClass(TweetHistogramJob.class);
    job.setMapperClass(TweetHistogramMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(TweetHistogramReducer.class);
    job.setMapOutputKeyClass(IntIntPair.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(3);
    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }

}

