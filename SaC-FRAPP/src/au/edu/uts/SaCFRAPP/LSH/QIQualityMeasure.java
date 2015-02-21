package au.edu.uts.SaCFRAPP.LSH;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class QIQualityMeasure extends Configured implements Tool{

	public QIQualityMeasure(){
		
	}
	
	private int measure(String dataPath) throws Exception{
		
		Configuration measureConf = getConf();
		Job measureJob = new Job(measureConf, "QIQualityMeasure");
		measureJob.setJarByClass(QIQualityMeasure.class);
		FileInputFormat.setInputPaths(measureJob, new Path(dataPath+"/recoding-output"));
		measureJob.setMapperClass(QIQualityMeasureMapper.class);
		measureJob.setMapOutputKeyClass(Text.class);
		measureJob.setMapOutputValueClass(DoubleWritable.class);
		measureJob.setReducerClass(QIQualityMeasureReducer.class);
		measureJob.setOutputKeyClass(Text.class);
		measureJob.setOutputValueClass(DoubleWritable.class);
		measureJob.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(measureJob, new Path(dataPath+"/measure-output"));
		
		measureJob.waitForCompletion(true);
		
		return 0;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		String dataPath = args[0].trim();
		
		this.measure(dataPath);
		
		return 0;
	}
	
	public static void main(String args[]){
		try{
			ToolRunner.run(new Configuration(), new QILocalRecoding(), args);
			ToolRunner.run(new Configuration(), new QIQualityMeasure(), args);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
