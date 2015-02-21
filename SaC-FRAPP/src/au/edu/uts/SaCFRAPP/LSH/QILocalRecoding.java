package au.edu.uts.SaCFRAPP.LSH;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class QILocalRecoding extends Configured implements Tool{

	public QILocalRecoding(){
		
	}
	
	private int recoding(String dataPath, int numOfReducers) throws Exception{
		Configuration recodingConf = getConf();
		Job recodingJob = new Job(recodingConf, "QILocalRecoding");
		recodingJob.setJarByClass(QILocalRecoding.class);
		FileInputFormat.setInputPaths(recodingJob, new Path(dataPath+"/clustering-output"));
		recodingJob.setMapperClass(QILocalRecodingMapper.class);
		recodingJob.setMapOutputKeyClass(Text.class);
		recodingJob.setMapOutputValueClass(Text.class);
		recodingJob.setReducerClass(QILocalRecodingReducer.class);
		recodingJob.setOutputKeyClass(Text.class);
		recodingJob.setOutputValueClass(Text.class);
		recodingJob.setNumReduceTasks(numOfReducers);
		FileOutputFormat.setOutputPath(recodingJob, new Path(dataPath+"/recoding-output"));
		
		recodingJob.waitForCompletion(true);
		
		return 0;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		String dataPath = args[0].trim();
		int numOfReducers = Integer.valueOf(args[1].trim());
		
		this.recoding(dataPath, numOfReducers);
		
		return 0;
	}
	
	public static void main(String args[]){
		try{
			ToolRunner.run(new Configuration(), new QILocalRecoding(), args);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
