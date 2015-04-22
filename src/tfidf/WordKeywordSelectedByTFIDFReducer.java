package tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordKeywordSelectedByTFIDFReducer extends Reducer<Text, Text, Text, Text> {

	public WordKeywordSelectedByTFIDFReducer()
	{
		
	}
	private static Text OUT_KEY = new Text();
	private static Text OUT_VALUE = new Text();

	/**
	 * PRE-CONDITION: receive a list of <document, ["word=n", "word-b=x"]> pairs
	 * <"a.txt", ["word1=3", "word2=5", "word3=5"]>
	 * 
	 * POST-CONDITION: <"word1@a.txt, 3/13">, <"word2@a.txt, 5/13">
	 * 
	 * @param key
	 *            is the key of the mapper
	 * @param values
	 *            are all the values aggregated during the mapping phase
	 * @param context
	 *            contains the context of the job run
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Map<String, Float> map = new HashMap<String, Float>();
	
		for (Text val : values) {
			String[] wordSelector = val.toString().split("=");
			map.put(wordSelector[0], Float.valueOf(wordSelector[1]));
		}
		
		
		List<Map.Entry<String, Float>> infoIds = new ArrayList<Map.Entry<String, Float>>();
		infoIds.addAll(map.entrySet());
		
        Collections.sort(infoIds, new Comparator<Map.Entry<String, Float>>() {  
            public int compare(Map.Entry<String, Float> o1,  
                    Map.Entry<String, Float> o2) {
            	if (o1.getValue()>o2.getValue())
            		return 1;
            	else {
					return -1;
				} 
            }  
        });  
  
        String outValue=null;
        int count=0;
        for(Iterator<Map.Entry<String,Float>> it=infoIds.iterator();it.hasNext();)  
        {  
        	if (count++ > 20)
        		break;
        	if (outValue==null)
        		outValue=it.next().getKey();
        	else
        		outValue=outValue+","+it.next().getKey(); 
        }  
        

		OUT_KEY.set(key.toString());
		OUT_VALUE.set(outValue);
		context.write(OUT_KEY, OUT_VALUE);

		
	}
}
