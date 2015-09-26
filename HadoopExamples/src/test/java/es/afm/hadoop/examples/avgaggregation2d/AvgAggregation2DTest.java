package es.afm.hadoop.examples.avgaggregation2d;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import es.afm.hadoop.examples.writables.AverageWritable;
import es.afm.hadoop.examples.writables.PointWritable;
import es.afm.hadoop.examples.writables.avgaggregation2d.AvgAggregation2DMapper;
import es.afm.hadoop.examples.writables.avgaggregation2d.AvgAggregation2DReducer;

public class AvgAggregation2DTest {
	MapDriver<LongWritable, Text, PointWritable, AverageWritable> mapDriver;
	ReduceDriver<PointWritable, AverageWritable, PointWritable, AverageWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, PointWritable, AverageWritable, PointWritable, AverageWritable> mapReduceDriver;

	@Before
	public void setUp() {
		AvgAggregation2DMapper mapper = new AvgAggregation2DMapper();
		AvgAggregation2DReducer reducer = new AvgAggregation2DReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMap() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("1,2:12"));

		mapDriver.withOutput(new PointWritable(1.0, 2.0), new AverageWritable(1, 12));

		mapDriver.runTest();
	}

	@Test
	public void testReduce() throws IOException {
		reduceDriver.addInput(new PointWritable(1.0, 2.0),
				Arrays.asList(new AverageWritable[] { new AverageWritable(1, 6), new AverageWritable(1, 4) }));

		reduceDriver.withOutput(new PointWritable(1.0, 2.0), new AverageWritable(2, 10));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("1,2:12"));
		mapReduceDriver.withInput(new LongWritable(), new Text("1,2:12"));
		mapReduceDriver.withInput(new LongWritable(), new Text("1,1:2"));

		mapReduceDriver.withOutput(new PointWritable(1.0, 1.0), new AverageWritable(1, 2));
		mapReduceDriver.withOutput(new PointWritable(1.0, 2.0), new AverageWritable(2, 24));

		mapReduceDriver.runTest();
	}
}
