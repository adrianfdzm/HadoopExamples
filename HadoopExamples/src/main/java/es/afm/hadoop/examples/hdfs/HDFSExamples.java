package es.afm.hadoop.examples.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class HDFSExamples extends Configured implements Tool {
	private static final String FILE_PATH = "/hdfsexamples/example1";
	private static final String DIR_PATH = "/hdfsexamples";
	private static final String LOCAL_PATH = "./example1";

	private FileSystem fs;

	public int run(String[] args) throws Exception {
		fs = FileSystem.get(getConf());
		createDir();
		writeSampleFile();
		readSampleFile();
		get();
		remove();
		put();
		return 0;
	}

	public HDFSExamples() throws IOException {
		
	}
	
	private void get() throws IllegalArgumentException, IOException {
		fs.copyToLocalFile(new Path(FILE_PATH), new Path(LOCAL_PATH));
	}
	
	private void remove() throws IllegalArgumentException, IOException {
		fs.delete(new Path(FILE_PATH), false);
	}
	
	private void put() throws IllegalArgumentException, IOException {
		fs.copyFromLocalFile(new Path(LOCAL_PATH), new Path(FILE_PATH));
	}

	private void writeSampleFile() throws IOException {
		FSDataOutputStream stream = fs.create(new Path(FILE_PATH), true);
		PrintWriter pw = new PrintWriter(stream);
		pw.println("Probando a escribir en el HDFS desde java");
		pw.close();
	}

	private void createDir() throws IOException {
		fs.mkdirs(new Path(DIR_PATH));
	}

	private void readSampleFile() throws IOException {
		FSDataInputStream stream = fs.open(new Path(FILE_PATH));
		BufferedReader br = new BufferedReader(new InputStreamReader(stream));
		String line = null;
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}
		br.close();
	}

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		ToolRunner.run(new HDFSExamples(), args);
	}

}
