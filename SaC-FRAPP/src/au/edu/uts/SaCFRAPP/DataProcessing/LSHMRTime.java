package au.edu.uts.SaCFRAPP.DataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class LSHMRTime extends ExperimentStatistics {

	String path;
	
	public LSHMRTime(String path) {
		super(10, 10);

        this.path = path;
	}

	@Override
	public void dataReading() {
		try{
			for(int i = 0; i<numOfVariables; i++){
				DescriptiveStatistics stats = new DescriptiveStatistics();
				for(int j = 0; j<numOfRounds; j++){
					String fileStr = path+"cluster-time-"+j+"-"+i;
					File file = new File(fileStr);
					if(!file.exists()){
						System.out.println("Warning: "+fileStr+" does not exist!");
					}
					else{
						BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
						String line = br.readLine();
						if(line == null){
							System.out.println("Error: "+fileStr+" contains nothing!");
							br.close();
							continue;
						}
						stats.addValue(Double.valueOf(line.trim()));
						
						br.close();
					}
				}
				counts[i] = stats.getN();
				means[i] = stats.getMean();
				sDeviations[i] = stats.getStandardDeviation();
				sErrors[i] = sDeviations[i]*1.96/Math.sqrt(counts[i]);
			}
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		LSHMRTime time4 = new LSHMRTime("rlt/LSH-exp4/exp4/");
		time4.dataReading();
		time4.displaystatistics();
		
		System.out.println();
		LSHMRTime time5 = new LSHMRTime("rlt/LSH-exp5/exp5/output/");
		time5.dataReading();
		time5.displaystatistics();

	}

}
