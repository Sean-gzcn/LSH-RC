package au.edu.uts.SaCFRAPP.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Count extends Configured implements Tool {
	
	private int count( String inputPath,
			String outputPath,
			int numOfReducers) throws Exception{
		
		Configuration countConf = getConf();
		Job countJob = new Job(countConf, "Count");
		countJob.setJarByClass(Count.class);
		FileInputFormat.setInputPaths(countJob, new Path(inputPath));
		countJob.setMapperClass(CountMapper.class);
		countJob.setMapOutputKeyClass(WritableRawKey.class);
		countJob.setMapOutputValueClass(IntWritable.class);
		countJob.setCombinerClass(CountCombiner.class);
		countJob.setPartitionerClass(CountPartitioner.class);
		countJob.setReducerClass(CountReducer.class);
		countJob.setNumReduceTasks(numOfReducers);
		countJob.setOutputKeyClass(Text.class);
		countJob.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(countJob, new Path(outputPath));
		
		countJob.waitForCompletion(true);
		
		System.out.println("Number of Wrong Records: "+countJob.getCounters().findCounter(UserCounters.WRONG_SIZE).getValue());
		System.out.println("Number of Incomplete Records: "+countJob.getCounters().findCounter(UserCounters.INCOMPLETE).getValue());
		
		return 0;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		String inputPath = args[0];
		String outputPath = args[1];
		int numOfReducers = Integer.valueOf(args[2].trim());
		
		this.count(inputPath, outputPath, numOfReducers);
		
		return 0;
	}
	
	public static void main(String[] args) {
		
		long start = System.nanoTime();
		try {
			int res = ToolRunner.run(new Configuration(), new Count(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Total Time: "+(System.nanoTime()-start)/1000000000.0);
	}
}
