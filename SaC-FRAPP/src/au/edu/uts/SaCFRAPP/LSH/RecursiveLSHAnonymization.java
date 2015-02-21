package au.edu.uts.SaCFRAPP.LSH;

import java.io.PrintWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class RecursiveLSHAnonymization implements Experiment{

	public RecursiveLSHAnonymization() {
		// TODO Auto-generated constructor stub
	}
	
	
	@Override
	public void run(String[] args) {
		
		long start = System.nanoTime();
		
		try{
			
			String[] minLSHParameters = new String[6];
			minLSHParameters[0] = args[0].trim();
			minLSHParameters[1] = args[1].trim();
			minLSHParameters[2] = args[2].trim();
			minLSHParameters[3] = args[3].trim();
			minLSHParameters[4] = args[4].trim();
			minLSHParameters[5] = args[5].trim();
			
			// Generate hash function family
			this.generateHashFuctionFamily(minLSHParameters[0], "hashFunctionFamily", Integer.valueOf(Integer.valueOf(minLSHParameters[2])));
			
			// MinHash based LSH
			ToolRunner.run(new Configuration(), new RecursiveMinLSH(), minLSHParameters);
			
			// Local recoding
			String[] recodingPara = new String[2];
			recodingPara[0] = args[0].trim();
			recodingPara[1] = args[5].trim();
			ToolRunner.run(new Configuration(), new QILocalRecoding(), recodingPara);
			
			this.timing(minLSHParameters[0], (System.nanoTime()-start)/1000000000.0);
			
			
			// Measure
			String[] measurePara = new String[1];
			measurePara[0] = args[0].trim();
			ToolRunner.run(new Configuration(), new QIQualityMeasure(), measurePara);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void generateHashFuctionFamily(String basePathStr, String functionFamilyPathStr, int numOfFuctions){
		
		try{
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			Path hdfsFile = new Path(basePathStr+"/"+functionFamilyPathStr);
			FSDataOutputStream out = hdfs.create(hdfsFile);
			PrintWriter pw = new PrintWriter(out);
				
			Random r = new Random();
			for(int i = 0; i<numOfFuctions; i++){
				pw.print(r.nextInt()+"\t");
				pw.print(r.nextInt()+"\t");
				pw.print(r.nextInt()+"\n");
				pw.flush();
			}
			pw.close();
			hdfs.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public void timing(String basePathStr, double seconds){
		
		try{
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			Path hdfsFile = new Path(basePathStr+"/time");
			FSDataOutputStream out = hdfs.create(hdfsFile);
			PrintWriter pw = new PrintWriter(out);
			pw.println(seconds);
			pw.flush();
			pw.close();
			hdfs.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		RecursiveLSHAnonymization anony = new RecursiveLSHAnonymization();
		
		long start = System.nanoTime();
		anony.run(args);
	    System.out.println("Total Time: "+(System.nanoTime()-start)/1000000000.0);
	}

	

}
