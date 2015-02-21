package au.edu.uts.SaCFRAPP.DataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class LSHExp7Iloss extends ExperimentStatistics {

	public LSHExp7Iloss() {
		super(10, 3);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void dataReading() {

		try{
			for(int i = 0; i<numOfVariables; i++){
				DescriptiveStatistics stats = new DescriptiveStatistics();
				for(int j = 0; j<numOfRounds; j++){
					String fileStr = "rlt/LSH-exp7/exp7/exp7-"+j+"/output/cluster-iloss-"+i;
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
						StringTokenizer st = new StringTokenizer(line);
						st.nextToken();
						stats.addValue(Double.valueOf(st.nextToken().trim())/((i+1)*1000000));
						
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
		LSHExp7Iloss iloss7 = new LSHExp7Iloss();
		iloss7.dataReading();
		iloss7.displaystatistics();

	}

}
