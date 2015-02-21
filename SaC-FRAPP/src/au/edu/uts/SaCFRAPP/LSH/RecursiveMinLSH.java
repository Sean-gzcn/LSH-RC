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
import org.apache.hadoop.util.ToolRunner;

public class RecursiveMinLSH extends Configured implements Tool {

	public RecursiveMinLSH() {
		// TODO Auto-generated constructor stub
	}
	
	private int recursiveMinLSH(String basePathStr, int paraK, int globalWidth, int localWidth, float weight, int numOfReducers) throws Exception{
		
		Configuration recMinLSHConf = getConf();
		Job recMinHashJob = new Job(recMinLSHConf, "RecursiveMinLSH");
		recMinHashJob.setJarByClass(RecursiveMinLSH.class);
		FileInputFormat.setInputPaths(recMinHashJob, new Path(basePathStr+"/input"));
		recMinHashJob.setMapperClass(MinLSHMapper.class);
		recMinHashJob.setMapOutputKeyClass(Text.class);
		recMinHashJob.setMapOutputValueClass(Text.class);
		//recMinHashJob.setReducerClass(MinLSHReducer.class);
		//recMinHashJob.setReducerClass(SimpleMinLSHReducer.class);
		//recMinHashJob.setReducerClass(KMemberReducer.class);
		//recMinHashJob.setReducerClass(GreedyKMemberReducer.class);
		recMinHashJob.setReducerClass(RecursiveMinLSHReducer.class);
		recMinHashJob.setNumReduceTasks(numOfReducers);
		recMinHashJob.setOutputKeyClass(Text.class);
		recMinHashJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(recMinHashJob, new Path(basePathStr+"/clustering-output"));
		
		recMinHashJob.getConfiguration().setInt("WIDTH", globalWidth);
		recMinHashJob.getConfiguration().setInt("WIDTH-LOCAL", localWidth);
		recMinHashJob.getConfiguration().setInt("K", paraK);
		recMinHashJob.getConfiguration().setFloat("WEIGHT", weight);
		recMinHashJob.getConfiguration().setInt("BANDS", 1);
		
		Path hashFamilyPath = new Path(basePathStr+"/hashFunctionFamily");
		DistributedCache.addCacheFile(hashFamilyPath.toUri(), recMinHashJob.getConfiguration()); // Note that the second parameter should not be.
		
		recMinHashJob.waitForCompletion(true);
		
		return 0;
	}

	

	@Override
	public int run(String[] args) throws Exception {
		
		String basePathStr = args[0];
		int paraK = Integer.valueOf(args[1]);
		int globalWidth = Integer.valueOf(args[2]);
		int localWidth = Integer.valueOf(args[3]);
		float weight = Float.valueOf(args[4]);
		int numOfReducers = Integer.valueOf(args[5]);
		
		this.recursiveMinLSH(basePathStr, paraK, globalWidth, localWidth, weight, numOfReducers);
		
		return 0;
	}
}
