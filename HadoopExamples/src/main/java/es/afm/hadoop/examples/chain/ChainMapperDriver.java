package es.afm.hadoop.examples.chain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import es.afm.hadoop.examples.wordcount.WordcountMapper;
import es.afm.hadoop.examples.wordcount.WordcountReducer;

public class ChainMapperDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("ChainMapperDriver required params: <input_path> <output_path>");
			System.exit(-1);
		}
		
		deleteOutputFileIfExists(args);
		final Job job = Job.getInstance();
		job.setJarByClass(ChainMapperDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		ChainMapper.addMapper(job, LowerCaseMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, new Configuration(false));
		ChainMapper.addMapper(job, FilterMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, new Configuration(false));
		ChainMapper.addMapper(job, WordcountMapper.class, LongWritable.class, Text.class, Text.class, LongWritable.class, new Configuration(false));
		
		job.setReducerClass(WordcountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return 0;
	}
	
	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		ToolRunner.run(new ChainMapperDriver(), args);
	}

}
