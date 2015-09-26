package es.afm.hadoop.examples.wordcount;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordcountTest {
	MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;

	@Before
	public void setup() {
		WordcountMapper mapper = new WordcountMapper();
		WordcountReducer reducer = new WordcountReducer();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, reducer);
	}

	@Test
	public void mapTest() throws IOException {
		LongWritable one = new LongWritable(1);

		mapDriver.withInput(new LongWritable(), new Text("Hola mundo Adios mundo"));

		mapDriver.addOutput(new Text("Hola"), one);
		mapDriver.addOutput(new Text("mundo"), one);
		mapDriver.addOutput(new Text("Adios"), one);
		mapDriver.addOutput(new Text("mundo"), one);

		mapDriver.runTest();
	}

	@Test
	public void reduceTest() throws IOException {
		reduceDriver.addInput(new Text("mundo"),
				Arrays.asList(new LongWritable[] { new LongWritable(1), new LongWritable(1) }));
		
		reduceDriver.withOutput(new Text("mundo"), new LongWritable(2));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void mapReduceTest() throws IOException {
		LongWritable one = new LongWritable(1);
		mapReduceDriver.addInput(new LongWritable(), new Text("Hola mundo"));
		mapReduceDriver.addInput(new LongWritable(), new Text("Adios mundo"));
		
		mapReduceDriver.addOutput(new Text("Adios"), one);
		mapReduceDriver.addOutput(new Text("Hola"), one);
		mapReduceDriver.addOutput(new Text("mundo"), new LongWritable(2));
		
		mapReduceDriver.runTest();
	}
	
	
}
