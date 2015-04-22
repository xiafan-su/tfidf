package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordKeywordSelectedByTFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

	public WordKeywordSelectedByTFIDFMapper()
	{
		
	}

	// Reuse writables
	private static Text INTERM_KEY = new Text();
	private static Text INTERM_VALUE = new Text();

	/**
	 * PRE-CONDITION: youtube@B000P3A55M \t [13/10202 , 1/578 , 0.0050082]
	 * 
	 * POST-CONDITION: key:B000P3A55M value:
	 * 
	 * @param key
	 *            is the byte offset of the current line in the file;
	 * @param value
	 *            is the line from the file
	 * @param context
	 *            the context of the job
	 * 
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] wordAndCounters = value.toString().split("\t");
		String[] wordAndDoc = wordAndCounters[0].split("@");
		
		
		String parameter=wordAndCounters[1].substring(1, wordAndCounters[1].length()-2);
		String tfidf=parameter.split(",")[2].trim();
		
		//key:songid value: word=0.04
		INTERM_KEY.set(wordAndDoc[1]);
		INTERM_VALUE.set(wordAndDoc[0] + "=" + tfidf);
		context.write(INTERM_KEY, INTERM_VALUE);
	}


}
