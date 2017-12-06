package uk.ac.qmul.reducer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.qmul.writable.IntIntPair;

public class TweetHistogramReducer extends Reducer<IntIntPair, IntWritable, IntIntPair, IntWritable> {

	private IntWritable result = new IntWritable();
	
	@Override
	public void reduce(IntIntPair key, Iterable<IntWritable> values, Context context)

			throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable value : values) {

			sum += value.get();

		}

			result.set(sum);

			context.write(key, result);


	}

}