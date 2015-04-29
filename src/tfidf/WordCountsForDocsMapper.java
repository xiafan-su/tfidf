package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountsForDocsMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	public WordCountsForDocsMapper() {
	}

	// Reuse writables
	private static Text INTERM_KEY = new Text();
	private static Text INTERM_VALUE = new Text();


	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] wordAndDocCounter = value.toString().split("\t");
		String[] wordAndDoc = wordAndDocCounter[0].split("@");

		INTERM_KEY.set(wordAndDoc[1]);
		INTERM_VALUE.set(wordAndDoc[0] + "=" + wordAndDocCounter[1]);
		context.write(INTERM_KEY, INTERM_VALUE);
	}
}
