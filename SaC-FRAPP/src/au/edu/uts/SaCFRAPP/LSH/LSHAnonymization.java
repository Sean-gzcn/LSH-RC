package au.edu.uts.SaCFRAPP.LSH;

import java.io.PrintWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;


public class LSHAnonymization {

	public LSHAnonymization(){
		
	}
	public void run(String args[]){
		
		try{
			// Generate hash function family
			String[] minLSHParameters = new String[6];
			minLSHParameters[0] = args[0].trim();
			minLSHParameters[1] = args[1].trim();
			minLSHParameters[2] = args[2].trim();
			this.generateHashFuctionFamily(minLSHParameters[0], "hashFunctionFamily", Integer.valueOf(minLSHParameters[1])*Integer.valueOf(minLSHParameters[2]));
			
			// MinHash based LSH
			minLSHParameters[3] = args[3].trim();
			minLSHParameters[4] = args[4].trim();
			minLSHParameters[5] = args[6].trim();
			ToolRunner.run(new Configuration(), new MinLSH(), minLSHParameters);
			
			// Remove copies
			String[] groupMergeParameters = new String[5];
			groupMergeParameters[0] = args[0].trim();
			groupMergeParameters[1] = args[3].trim();
			groupMergeParameters[2] = args[4].trim();
			
			int ROUND = Integer.valueOf(args[5].trim());
			int round = 0;
			while(round < ROUND){
				groupMergeParameters[3] = String.valueOf(round);
				ToolRunner.run(new Configuration(), new GroupMerge(), groupMergeParameters);
				round++;
			}
			
			//
			
			// Local recoding
			String[] recodingPara = new String[1];
			recodingPara[0] = args[0].trim();
			ToolRunner.run(new Configuration(), new QILocalRecoding(), recodingPara);
			
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
	
	public static void main(String[] args){
		
		//System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
		
		LSHAnonymization anony = new LSHAnonymization();
		
		long start = System.nanoTime();
		
		anony.run(args);
		
	    System.out.println("Total Time: "+(System.nanoTime()-start)/1000000000.0);
	}
}
