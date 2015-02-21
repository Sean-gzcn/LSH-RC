package au.edu.uts.SaCFRAPP.LSH;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;

public class MinLSH extends Configured implements Tool{

	public MinLSH(){
		
	}
	
	private int minLSH(String basePathStr, int numOfBands, int widthOfBand, int numOfReducers, int hashTableSize, int paraK) throws Exception{
		Configuration minLSHConf = getConf();
		Job minHashJob = new Job(minLSHConf, "MinLSH");
		minHashJob.setJarByClass(MinLSH.class);
		FileInputFormat.setInputPaths(minHashJob, new Path(basePathStr+"/input"));
		minHashJob.setMapperClass(MinLSHMapper.class);
		minHashJob.setMapOutputKeyClass(Text.class);
		minHashJob.setMapOutputValueClass(Text.class);
		//minHashJob.setReducerClass(MinLSHReducer.class);
		//minHashJob.setReducerClass(SimpleMinLSHReducer.class);
		//minHashJob.setReducerClass(KMemberReducer.class);
		minHashJob.setReducerClass(GreedyKMemberReducer.class);
		minHashJob.setNumReduceTasks(numOfReducers);
		minHashJob.setOutputKeyClass(Text.class);
		minHashJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(minHashJob, new Path(basePathStr+"/clustering-output"));
		
		minHashJob.getConfiguration().setInt("BANDS", numOfBands);
		minHashJob.getConfiguration().setInt("WIDTH", widthOfBand);
		minHashJob.getConfiguration().setInt("HashTableSize", hashTableSize);
		minHashJob.getConfiguration().setInt("K", paraK);
		
		Path hashFamilyPath = new Path(basePathStr+"/hashFunctionFamily");
		DistributedCache.addCacheFile(hashFamilyPath.toUri(), minHashJob.getConfiguration()); // Note that the second parameter should not be.
		
		minHashJob.waitForCompletion(true);
		
		System.out.println("Number of original Groups: "+minHashJob.getCounters().findCounter(GroupCounters.ORIGINAL_GROUPS).getValue());
		System.out.println("Number of refined Groups: "+minHashJob.getCounters().findCounter(GroupCounters.REFINED_GROUPS).getValue());
		return 0;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		String basePathStr = args[0];
		int numOfBands = Integer.valueOf(args[1]);
		int widthOfBand = Integer.valueOf(args[2]);
		int numOfReducers = Integer.valueOf(args[3]);
		int hashTableSize = Integer.valueOf(args[4]);
		int paraK = Integer.valueOf(args[5]);
		
		this.minLSH(basePathStr, numOfBands, widthOfBand, numOfReducers, hashTableSize, paraK);
		
		return 0;
	}
}
