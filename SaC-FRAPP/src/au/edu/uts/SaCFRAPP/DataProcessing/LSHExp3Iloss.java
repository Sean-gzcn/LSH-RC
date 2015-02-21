package au.edu.uts.SaCFRAPP.DataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class LSHExp3Iloss extends ExperimentStatistics {

	public LSHExp3Iloss() {
		super(10, 10);
	}

	@Override
	public void dataReading() {
		try{
			for(int i = 0; i<numOfVariables; i++){
				DescriptiveStatistics stats = new DescriptiveStatistics();
				for(int j = 0; j<numOfRounds; j++){
					String fileStr = "rlt/LSH-exp3/exp3/exp3-"+j+"/output/cluster-0-iloss-"+i;
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
						stats.addValue(Double.valueOf(st.nextToken().trim())/((i+1)*100000));
						
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
		
		LSHExp3Iloss loss3 = new LSHExp3Iloss();
		loss3.dataReading();
		loss3.displaystatistics();

	}

}
