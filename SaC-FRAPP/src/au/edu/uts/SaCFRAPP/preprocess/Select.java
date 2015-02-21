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


public class Select extends Configured implements Tool {
	
	private int select ( String inputPath,
			String outputPath) throws Exception{
		
		Configuration selectConf = getConf();
		Job selectJob = new Job(selectConf, "Select");
		selectJob.setJarByClass(Select.class);
		FileInputFormat.setInputPaths(selectJob, new Path(inputPath));
		selectJob.setMapperClass(SelectMapper.class);
		selectJob.setMapOutputKeyClass(IntWritable.class);
		selectJob.setMapOutputValueClass(Text.class);
		selectJob.setReducerClass(SelectReducer.class);
		selectJob.setOutputKeyClass(Text.class);
		selectJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(selectJob, new Path(outputPath));
		
		selectJob.waitForCompletion(true);
		
		return 0;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		String inputPath = args[0];
		String outputPath = args[1];
		
		this.select(inputPath, outputPath);
		
		return 0;
	}
	
	public static void main(String[] args) {
		
		long start = System.nanoTime();
		try {
			int res = ToolRunner.run(new Configuration(), new Select(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Total Time: "+(System.nanoTime()-start)/1000000000.0);
	}
}