package tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountsForDocsReducer extends Reducer<Text, Text, Text, Text> {

	public WordCountsForDocsReducer() {
	}

	private static Text OUT_KEY = new Text();
	private static Text OUT_VALUE = new Text();


	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sumOfWordsInDocument = 0;
		Map<String, Integer> tempCounter = new HashMap<String, Integer>();

		for (Text val : values) {
			String[] wordCounter = val.toString().split("=");
			tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
			sumOfWordsInDocument += Integer.parseInt(wordCounter[1]);
		}

		for (Entry<String, Integer> entry : tempCounter.entrySet()) {
			OUT_KEY.set(entry.getKey() + "@" + key.toString());
			OUT_VALUE.set(entry.getValue() + "/" + sumOfWordsInDocument);
			context.write(OUT_KEY, OUT_VALUE);
		}
	}
}
