package es.afm.hadoop.examples.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class NGramsCountDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("NGramsCountDriver required params: <input_path> <output_path> <ngram-size>");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = Job.getInstance(getConf());
		job.setJarByClass(NGramsCountDriver.class);
		job.setInputFormatClass(SentenceInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(NGramsCountMapper.class);
		job.setReducerClass(NGramsCountReducer.class);
		// We may use a combiner
		job.setCombinerClass(NGramsCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// pass ngramsize to map and reduce
		try {
			job.getConfiguration().setInt("ngram.size",
					Integer.parseInt(args[2]));
		} catch (NumberFormatException e) {
			System.err.println("Ngram-size is expected to be an integer value");
			return 0;
		}
		job.waitForCompletion(true);

		return 0;
	}

	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		ToolRunner.run(new NGramsCountDriver(), args);
	}
}
