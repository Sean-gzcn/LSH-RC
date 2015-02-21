package au.edu.uts.SaCFRAPP.LSH;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class GroupMerge extends Configured implements Tool{

	public GroupMerge(){
		
	}
	
	private int groupMerge(String basePathStr, int numOfReducers, int hashTableSize, int round) throws Exception{
		Configuration mergeConf = getConf();
		Job mergeJob = new Job(mergeConf, "Group Merge");
		mergeJob.setJarByClass(GroupMerge.class);
		FileInputFormat.setInputPaths(mergeJob, new Path(basePathStr+"/merge-output-"+round));
		mergeJob.setMapperClass(GroupMergeMapper.class);
		mergeJob.setMapOutputKeyClass(Text.class);
		mergeJob.setMapOutputValueClass(Text.class);
		mergeJob.setReducerClass(MinLSHReducer.class);
		mergeJob.setNumReduceTasks(numOfReducers);
		mergeJob.setOutputKeyClass(Text.class);
		mergeJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(mergeJob, new Path(basePathStr+"/merge-output-"+(round+1)));
	
		mergeJob.getConfiguration().setInt("HashTableSize", hashTableSize);
		
		mergeJob.waitForCompletion(true);
		
		System.out.println("Number of original Groups: "+mergeJob.getCounters().findCounter(GroupCounters.ORIGINAL_GROUPS).getValue());
		System.out.println("Number of refined Groups: "+mergeJob.getCounters().findCounter(GroupCounters.REFINED_GROUPS).getValue());
		return 0;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		String basePathStr = args[0];
		int numOfReducers = Integer.valueOf(args[1]);
		int hashTableSize = Integer.valueOf(args[2]);
		int round = Integer.valueOf(args[3]);
		
		this.groupMerge(basePathStr, numOfReducers, hashTableSize, round);
		
		return 0;
	}
}
