package au.edu.uts.SaCFRAPP.DataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class LSHExp3Time extends ExperimentStatistics {

	public LSHExp3Time() {
		super(10, 10);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void dataReading() {
		try{
			double[][] tmpTime = new double[numOfRounds][numOfVariables];
			for(int i = 0; i<numOfRounds; i++){
				for(int j = 0; j<numOfVariables; j++){
					tmpTime[i][j] = -1;
				}
			}
			
			for(int i = 0; i<numOfRounds; i++){
				String fileStr = "rlt/LSH-exp3/exp3/exp3-"+i+"/output/cluster-time";
				File file = new File(fileStr);
				if(!file.exists()){
					System.out.println("Error: "+fileStr+" does not exist!");
				}
				else{
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
					String line;
					int j = 0;
					while((line = br.readLine())!= null){
						tmpTime[i][j] = Double.valueOf(line.trim());
						j++;
					}
					br.close();
				}
			}
			
			for(int i = 0; i<numOfVariables; i++){
				DescriptiveStatistics stats = new DescriptiveStatistics();
				for (int j = 0; j<numOfRounds; j++){
					if(tmpTime[j][i] < 0){
						System.out.println("Error: time vlaue missed!");
					}
					stats.addValue(tmpTime[j][i]);
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
		LSHExp3Time time3 = new LSHExp3Time();
		time3.dataReading();
		time3.displaystatistics();

	}

}
