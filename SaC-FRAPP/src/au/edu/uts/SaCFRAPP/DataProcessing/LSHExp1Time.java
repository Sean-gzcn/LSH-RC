package au.edu.uts.SaCFRAPP.DataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class LSHExp1Time extends ExperimentStatistics {

	public LSHExp1Time() {
		super(10, 10);
	}

	@Override
	public void dataReading() {
		
		try{
			double[][] tmpTime = new double[numOfRounds][numOfVariables];
			for(int i = 0; i<numOfRounds; i++){
				String fileStr = "rlt/LSH-exp1/exp1/completed_0_"+i+"/output/agglom/cluster_1000-time";
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
	
	public void displayGreedy(){
		try{
			DescriptiveStatistics stats = new DescriptiveStatistics();
			for(int j = 0; j<numOfRounds; j++){
				String fileStr = "rlt/LSH-exp1/exp1/completed_0_"+j+"/output/greedy/cluster_1000-time";
				File file = new File(fileStr);
				if(!file.exists()){
					System.out.println("Warning: "+fileStr+" does not exist!");
				}
				else{
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
					String line = br.readLine();
					if(line == null){
						System.out.println("Error! The file contains nothing!");
					}
					stats.addValue(Double.valueOf(line.trim()));
					
					br.close();
				}
			}
			System.out.println("\n\n"+stats.getN()+"\t"+stats.getMean()+"\t"+stats.getStandardDeviation()+"\t"+stats.getStandardDeviation()*1.96/Math.sqrt(stats.getN()));
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		LSHExp1Time time = new LSHExp1Time();
		time.dataReading();
		time.displaystatistics();
		time.displayGreedy();
	}

}
