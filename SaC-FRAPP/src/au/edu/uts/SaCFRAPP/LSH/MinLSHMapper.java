package au.edu.uts.SaCFRAPP.LSH;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.BitSet;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.edu.uts.SaCFRAPP.PAClustering.WritableRecord;

public class MinLSHMapper extends Mapper<LongWritable, Text, Text, Text> {

	MinHash minHash;
	int numOfHashFunctions;
	int bands;
	int widthOfBand;
	int[][] hashFamily;
	
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		
		minHash = new MinHash();
		bands = context.getConfiguration().getInt("BANDS", -1);
		widthOfBand = context.getConfiguration().getInt("WIDTH", -1);
		numOfHashFunctions = bands*widthOfBand;
		
		hashFamily = new int[numOfHashFunctions][3];
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if(cacheFiles != null && cacheFiles.length > 0){
			
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFiles[0].toString())));
			String line;
			int i = 0;
			while((line = br.readLine()) != null){
				StringTokenizer itr = new StringTokenizer(line, "\t");
				for(int j = 0; j<3; j++){
					hashFamily[i][j] = Integer.valueOf(itr.nextToken().trim());
				}
				i++;
    		}
    		br.close();
		}
		else{
			System.err.println("Error: no cache files! Seeds have not been passed!!!");
		}
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		for(int i = 0; i<bands; i++){
			String bucketID = "";
			int j = 0;
			for(; j<widthOfBand-1; j++){
				int mHash = minHash.minHash(value.toString().trim(), hashFamily[i*widthOfBand+j][0], 
						hashFamily[i*widthOfBand+j][1], hashFamily[i*widthOfBand+j][2]);
				bucketID = bucketID+mHash+"-";
			}
			int mHash = minHash.minHash(value.toString().trim(), hashFamily[i*widthOfBand+j][0], 
					hashFamily[i*widthOfBand+j][1], hashFamily[i*widthOfBand+j][2]);
			bucketID = bucketID+mHash;
			
			context.write(new Text(bucketID), value);
		}
	}
}
