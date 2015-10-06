# HadoopExamples
This code was built in order to be used as example of all the characteristic of Hadoop MapReduce framework. Next, there is a brief description of each example.
* **Wordcount**: The most basic example of MapReduce job
* **writables/Aggregation2D**: This example aggregates 2-dimension measurements in order to show how to use Hadoop writables
* **writables/AvgAggregation2D**: Another example on 2-dimension measurements that show the use of Hadoop writables
* **partitioner/UrlCount**: This example shows how partitioning functionality can be overriden in order group keys in the desired reduce task. The urls from a file are counted but we desire that urls with the same host end up in the same output file
* **partitioner/TotalOrderV1**: It shows how to order a wordcount file by the number of occurrences. Hadoop orders the output of each reduce task but we want the output of all reduce task to be ordered (part-r-00000 registers < part-r-00001 registers < part-r-00002 registers < ...). This job shows an ugly solution for a 2 reduce tasks job
* **partitioner/TotalOrderV2**: Try to solve the same problem exposed above but this time Hadoop ```Sampler``` and ```TotalOrderPartitioner``` classes are used
* **ReduceSideJoin**: Performs a join between two datasets using ```MultipleInputs``` to set a different ```Mapper``` class for each dataset. They map fucntion writes as key the value of the join key field, so results are grouped by the framework in the reduce phase

##Wordcount
Basic example of MapReduce job. Map split lines into words that are writen as key. Punctuation marks are not taken into account
```java
  	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String[] splits = value.toString().trim().split("\\s+");
		for (String split : splits) {
			context.write(new Text(split), one);
		}
	}
```
Reduce aggregates the number of occurrences of each word
```java
	 @Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long count = 0;
		for (LongWritable value : values) {
			count += value.get();
		}
		context.write(key, new LongWritable(count));
	}
```
WordcountReducer class is used as a combiner to aggreagate map output locally
```java
job.setCombinerClass(WordcountReducer.class);
```
In order to run the MapReduce job using ```hadoop jar``` command, main class musts implement ```Tool``` interface and extend ```Configured```. This is shown on ```WordcountDriver``` class

##writables/Aggregation2D

This MapReduce job aims to illustrate how to use Hadoop custom ```WritableComparable``` class inside a MapReduce job. Map coordinates that are readed from a plain text file are seriealized into  ```PointWritable``` objects during the map tasks and passed on to the reduce tasks
```java
  	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PointWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {

		try {
			String pointStr = value.toString().split(KEY_VALUE_SEP)[0];
			LongWritable outValue = new LongWritable(Long.parseLong(value.toString().split(KEY_VALUE_SEP)[1]));

			PointWritable outKey = new PointWritable(Double.parseDouble(pointStr.split(POINT_COORDINATE_SEP)[0]),
					Double.parseDouble(pointStr.split(POINT_COORDINATE_SEP)[1]));

			context.write(outKey, outValue);
		} catch (NumberFormatException e) {
			context.getCounter(Counters.WRONG_VALUE).increment(1);
		} catch (ArrayIndexOutOfBoundsException e) {
			context.getCounter(Counters.MALFORMED_RECORD).increment(1);
		}
	}
```

Note that error handling are performed incrementing some Hadoop counters. Counters are automatically aggregated over Map and Reduce phases. They are defined using java ```Enumeration```. The most natural use of that is to use counters to track the number of malformed records. But counters can actually be used for any kind of other statistics on your records. ```PointWritable``` class tells Hadoop how the 2-dimension information that is being handled has to be serialized during the MapReduce job
```java
public class PointWritable implements WritableComparable<PointWritable> {
	private DoubleWritable first;
	private DoubleWritable second;
	
	public PointWritable(double first, double second) {
		this.first = new DoubleWritable(first);
		this.second = new DoubleWritable(second);
	}
	
	/*
	 * init method without params must be present
	 */
	public PointWritable() {
		first = new DoubleWritable();
		second = new DoubleWritable();
	}
	
	public DoubleWritable getFirst() {
		return first;
	}

	public void setFirst(DoubleWritable first) {
		this.first = first;
	}

	public DoubleWritable getSecond() {
		return second;
	}

	public void setSecond(DoubleWritable second) {
		this.second = second;
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public int compareTo(PointWritable arg0) {
		if(first.compareTo(arg0.getFirst()) == 0)
			return second.compareTo(arg0.getSecond());
		return first.compareTo(arg0.getFirst());
	}
	
	@Override
	public String toString() {
		return "[" + first.get() + ", " + second.get() + "]";
	}
	
	/*MRUnit needs hashcode and equals in order to compare outputs*/
	@Override
	public int hashCode() {
		return first.hashCode()*128 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PointWritable){
			if(this.compareTo((PointWritable) obj) == 0)
				return true;
		}
		return false;
	}

}
```
Note that:
* A default constructor without arguments must be present because it is used by the MapReduce framework. 
* There is a ```Writable``` interface but, in order to use our custom ```Writable``` as key, it is needed to implement the ```WritableComparable``` interface
* ```equals()``` and ```hashCode()``` methods are used by some tools such as MRUnit (Unit testing for Hadoop MapReduce)
* ```toString()``` method is used to write to the final output file of the job

##writables/AvgAggregation2D

Based on the previous example, the same data is processed to calculate the average value of each point. Map phase produces now the pair ```<PointWritable,AverageWritable>```

```java
	String pointStr = value.toString().split(KEY_VALUE_SEP)[0];
	AverageWritable outValue = new AverageWritable(1, 
		Long.parseLong(value.toString().split(KEY_VALUE_SEP)[1]));

	PointWritable outKey = new PointWritable(Double.parseDouble(pointStr.split(POINT_COORDINATE_SEP)[0]),
		Double.parseDouble(pointStr.split(POINT_COORDINATE_SEP)[1]));
```

```AverageWritable``` implements ```Writable``` interface to store and serialize the number of occurrences of a point and the sum of his measurement so the average can be calculated even when the data goes through a combiner phase

```java
public class AverageWritable implements Writable {
	private LongWritable count;
	private LongWritable sum;

	public AverageWritable(long count, long sum) {
		this.count = new LongWritable(count);
		this.sum = new LongWritable(sum);
	}

	// init method without params must be present
	public AverageWritable() {
		count = new LongWritable();
		sum = new LongWritable();
	}

	public LongWritable getCount() {
		return count;
	}

	public void setCount(LongWritable count) {
		this.count = count;
	}

	public LongWritable getSum() {
		return sum;
	}

	public void setSum(LongWritable sum) {
		this.sum = sum;
	}

	public double getAverage() {
		if (count.get() > 0)
			return sum.get() / (double) count.get();
		return 0.0;
	}

	public void readFields(DataInput in) throws IOException {
		count.readFields(in);
		sum.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		count.write(out);
		sum.write(out);
	}

	@Override
	public String toString() {
		return String.valueOf(this.getAverage());
	}

	/* MRUnit needs hashcode and equals in order to compare outputs */
	@Override
	public int hashCode() {
		return count.hashCode() * 128 + sum.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AverageWritable) {
			AverageWritable avg = (AverageWritable) obj;
			if (this.count.get() == avg.count.get() && this.sum.get() == avg.sum.get())
				return true;
		}
		return false;
	}

}
```

##partitioner/UrlCount
A file containing a url in each line is read to count the number of occurrences of each url. Each reduce task in a Hadoop MapReduce job write its own output file. Several reduce tasks are used in this job and it is required that urls with the same host end up in the same output file so a custom ```Partitioner``` implementation is used.

```java
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("URLCountDriver required params: <input_path> <output_path>");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = new Job(getConf());
		//local mode execution will turn this to 1
		job.setNumReduceTasks(2);
		
		job.setJarByClass(URLCountDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(URLCountMapper.class);
		job.setReducerClass(URLCountReducer.class);
		// We may use a combiner
		job.setCombinerClass(URLCountReducer.class);
		job.setPartitionerClass(URLCountPartitioner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}
```

Note that the number of reduce tasks is set with ```job.setNumReduceTasks(2);``` call and the custom ```Partition``` implementation is set with ```job.setPartitionerClass(URLCountPartitioner.class)```

```java
public class URLCountPartitioner extends Partitioner<Text, LongWritable>{

	@Override
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		System.out.println(numPartitions);
		if(numPartitions == 0)
			return 0;
		try {
			URL url = new URL(key.toString());
			return url.getHost().hashCode()%numPartitions;
		} catch (MalformedURLException ignored) {
			return 0;
		}
	}

}
```

##partitioner/TotalOrderSortV1

The output from wordcount example are ordered alphabetically but now we desire to order that output by number of occurrences of each word. We preprocess the wordcount output file to change the key-value order of each register so the number of occurrences is now the key and the word is the value.

```java
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("PreprocessorDriver required params: <input_path> <output_path>");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = new Job(getConf());
		job.setJarByClass(PreProcessorDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(PreProcessorMapper.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.waitForCompletion(true);

		return 0;
	}
```

No reduce phase is needed so ```job.setNumReduceTasks(0);``` is called. The output of this job is going to be stored in the HDFS as a sequence file: ```job.setOutputFormatClass(SequenceFileOutputFormat.class);```. ```PreProcessorMapper``` is trivial so it is omitted here

Next job will order the output. If partitioning function is not overriden and more than one reduce task is set the output of the job would be similar to this example:

**part-r-00000**
```
1 hello
4 dog
6 world
```

**part-r-00001**
```
2 bye
3 cat
5 bunny
```

Output files are ordered localy but a total order is desired so #occurrences in part-r-00000 words < #ocurrences in part-r-00001 words:

**part-r-00000**
```
1 hello
2 bye
3 cat
```

**part-r-00001**
```
4 dog
5 bunny
6 world
```

A custom partitioning function is set through ```job.setPartitionerClass(TotalOrderV1Partitioner.class);``` call. In the job configuration, Mapper and Reducer implementations are not being set since sorting is always performed as a default.

```java
	@Override
	public int getPartition(LongWritable key, Text value, int numPartitions) {
		if(numPartitions == 0)
			return 0;
		//Hardcoded. Pretty ugly
		if(key.get()> 3)
			return 1;
		return 0;
	}
```
It is being assuming that 2 reduce task are going to be set. Pairs with 3 or less number of occurrences are put into the partition #0 and the other into the second one. It is a bad practise to hardcode like that but it is just for understanding. Next example will fix this.

##partitioner/TotalOrderSortV2

This example solves the same problem decipted below but in a more appropiate way. First Hadoop ```TotalOrderPartitioner``` class is set. Then ```Sampler``` utility is use in order to create a partition list from ramdom samples of the input provided by ```RamdomSampler``` class. This list is going to be use by the ```TotalOrderPartitioner``` class. ```InputSampler.writePartitionFile(job, sampler)``` call must be done after ```TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[1] + "/_partition.lst"));``` call

```java
	job.setPartitionerClass(TotalOrderPartitioner.class);
	RandomSampler<LongWritable, Text> sampler = new InputSampler.RandomSampler<LongWritable, Text>(0.1, 15);
	TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[1] + "/_partition.lst"));
	InputSampler.writePartitionFile(job, sampler);
```

##ReduceSideJoin

This example depicts how to perform a join between two dataset. First dataset contains several row in CSV format each one containing information about a teacher: identifier, department-id, name and salary. Second dataset contains information about deparments: department-id and department name. A teacher is related to only one department and a department has many associated teachers. The output of the job will be a row for each teacher containing his name, department name and salary. Then, department-id is going to be used as join key on the map phase. To make things easiers we are going to point the dataset the record belongs to adding a dataset key to map output records' key. So we have a composite key formed by the join key plus the data set key
```java
public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
	private Text joinKey;
	private IntWritable datasetKey;
	public static final int DATASET_KEY_TEACHERS = 2;
	public static final int DATASET_KEY_DEPARTMENTS = 1;

	/* Empty constructor needed */
	public CompositeKeyWritable() {
		joinKey = new Text();
		datasetKey = new IntWritable();
	}

	public CompositeKeyWritable(Text joinKey, IntWritable datasetKey) {
		this.joinKey = joinKey;
		this.datasetKey = datasetKey;
	}

	public Text getJoinKey() {
		return joinKey;
	}

	public void setJoinKey(Text joinKey) {
		this.joinKey = joinKey;
	}

	public IntWritable getDatasetKey() {
		return datasetKey;
	}

	public void setDatasetKey(IntWritable datasetKey) {
		this.datasetKey = datasetKey;
	}

	public void readFields(DataInput in) throws IOException {
		joinKey.readFields(in);
		datasetKey.readFields(in);

	}

	public void write(DataOutput out) throws IOException {
		joinKey.write(out);
		datasetKey.write(out);
	}

	public int compareTo(CompositeKeyWritable obj) {
		int result = joinKey.compareTo(obj.joinKey);
		if (result == 0)
			result = datasetKey.compareTo(obj.datasetKey);
		return result;
	}

	@Override
	public String toString() {
		return new StringBuilder().append(datasetKey).append(", ")
				.append(joinKey).toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof CompositeKeyWritable)
			return this.compareTo((CompositeKeyWritable) obj) == 0;
		return false;
	}
}
```
The job has to partition the record by its join key:
```java
public class ReduceSideJoinPartitioner extends Partitioner<CompositeKeyWritable, Text> {
	@Override
	public int getPartition(CompositeKeyWritable key, Text value, int numPartitions) {
		return Math.abs(key.getJoinKey().hashCode()) % numPartitions;
	}
}
```
Sort then by the whole composite key (```compareTo()``` method inside ```CompositeKeyWritable``` class):
```java
public int compareTo(CompositeKeyWritable obj) {
	int result = joinKey.compareTo(obj.joinKey);
	if (result == 0)
		result = datasetKey.compareTo(obj.datasetKey);
	return result;
}
```
Grouping is performed using a custom ```RawComparator``` implementations that group the records by its join key
```java
public class CompositeKeyWritableGroupingComparator extends WritableComparator {
	protected CompositeKeyWritableGroupingComparator() {
		super(CompositeKeyWritable.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKeyWritable w1 = (CompositeKeyWritable) a;
		CompositeKeyWritable w2 = (CompositeKeyWritable) b;
		return w1.getJoinKey().compareTo(w2.getJoinKey());
	}
}
```
```MultipleInput``` is used to associate a different mapper implementation to each dataset:
```java
MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TeachersMapper.class);
MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DepartmentsMapper.class);
```
