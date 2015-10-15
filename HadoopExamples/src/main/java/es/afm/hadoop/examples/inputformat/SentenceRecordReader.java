package es.afm.hadoop.examples.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SentenceRecordReader extends RecordReader<LongWritable, Text> {

	private SentenceReader in;
	private long start;
	private long end;
	private long pos;
	private Text value = new Text();
	private LongWritable key = new LongWritable();

	@Override
	public void close() throws IOException {
		if (in != null)
			in.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// This InputSplit is a FileInputSplit
		FileSplit split = (FileSplit) genericSplit;

		// Retrieve configuration, and Max allowed
		// bytes for a single record
		Configuration job = context.getConfiguration();

		// Split "S" is responsible for all records
		// starting from "start" and "end" positions
		start = split.getStart();
		end = start + split.getLength();

		// Retrieve file containing Split "S"
		final Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

		// If Split "S" starts at byte 0, first line will be processed
		// If Split "S" does not start at byte 0, first line has been already
		// processed by "S-1" and therefore needs to be silently ignored
		boolean skipFirstLine = false;
		if (start != 0) {
			skipFirstLine = true;
			// Set the file pointer at "start - 1" position.
			// This is to make sure we won't miss any line
			// It could happen if "start" is located on a EOL
			--start;
			fileIn.seek(start);
		}

		in = new SentenceReader(fileIn);

		// If first line needs to be skipped, read first line
		// and stores its content to a dummy Text
		if (skipFirstLine) {
			Text dummy = new Text();
			// Reset "start" to "start + line offset"
			start += in.readSentence(dummy);
		}

		// Position is the actual start
		this.pos = start;

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// Current offset is the key
		key.set(pos);

		int newSize = 0;

		// Read first line and store its content to "value"
		newSize = in.readSentence(value);

		// Line is read, new position is set
		pos += newSize;

		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

}
