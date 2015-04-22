package tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordKeywordSelectedByTFIDF selects the keywords in each document 
 * (Hadoop 0.20.2 API)
 * 
 * @author Demonsu (xiafan@hku.hk)
 */
public class WordKeywordSelectedByTFIDF extends Configured implements Tool {

	/**
	 * Setup the MR job
	 * 
	 * @param args
	 * @throws Exception
	 */
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.println("Usage: tf-idf-4 <tf-idf-1-output> <tf-idf-2-output>");
			System.exit(-1);
		}

		Configuration conf = getConf();
		Job job = new Job(conf, "Select Keywords");

		job.setJarByClass(WordKeywordSelectedByTFIDF.class);
		job.setMapperClass(WordKeywordSelectedByTFIDFMapper.class);
		job.setReducerClass(WordKeywordSelectedByTFIDFReducer.class);
		int reduceNum = 1;
		reduceNum = Integer.parseInt(args[2].split(",")[3]);
		job.setNumReduceTasks(reduceNum);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Main driver
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordKeywordSelectedByTFIDF(),
				args);
		System.exit(res);
	}

}
