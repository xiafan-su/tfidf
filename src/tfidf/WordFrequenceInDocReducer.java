
package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordFrequenceInDocReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private static IntWritable COUNT = new IntWritable();

	public WordFrequenceInDocReducer() {
	}


	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		// write the key and the adjusted value
		COUNT.set(sum);
		context.write(key, COUNT);
	}
}
