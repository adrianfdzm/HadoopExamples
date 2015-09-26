package es.afm.hadoop.examples.aggregation2d;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import es.afm.hadoop.examples.writables.PointWritable;
import es.afm.hadoop.examples.writables.aggregation2d.Aggregation2DMapper;
import es.afm.hadoop.examples.writables.aggregation2d.Aggregation2DReducer;

public class Aggregation2DTest {
	MapDriver<LongWritable, Text, PointWritable, LongWritable> mapDriver;
	ReduceDriver<PointWritable, LongWritable, PointWritable, LongWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, PointWritable, LongWritable, PointWritable, LongWritable> mapReduceDriver;

	@Before
	public void setUp() {
		Aggregation2DMapper mapper = new Aggregation2DMapper();
		Aggregation2DReducer reducer = new Aggregation2DReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMap() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("1,2:12"));

		mapDriver.withOutput(new PointWritable(1.0, 2.0), new LongWritable(12));

		mapDriver.runTest();
	}

	@Test
	public void testReduce() throws IOException {
		reduceDriver.addInput(new PointWritable(1.0, 2.0),
				Arrays.asList(new LongWritable[] { new LongWritable(6), new LongWritable(4) }));

		reduceDriver.withOutput(new PointWritable(1.0, 2.0), new LongWritable(10));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("1,2:12"));
		mapReduceDriver.withInput(new LongWritable(), new Text("1,2:12"));
		mapReduceDriver.withInput(new LongWritable(), new Text("1,1:2"));
		
		mapReduceDriver.withOutput(new PointWritable(1.0, 1.0), new LongWritable(2));
		mapReduceDriver.withOutput(new PointWritable(1.0, 2.0), new LongWritable(24));
		
		mapReduceDriver.runTest();
	}
}
