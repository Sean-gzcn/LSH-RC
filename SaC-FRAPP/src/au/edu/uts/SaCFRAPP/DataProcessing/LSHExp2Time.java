package au.edu.uts.SaCFRAPP.DataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class LSHExp2Time extends ExperimentStatistics {

	int widthOfBand;
	int WIDTH = 5;
	
	public LSHExp2Time(int widthOfBand) {
		super(10, 10);
		
		this.widthOfBand = widthOfBand;
	}
	
	public int[] getSkips(int round){
		int[] skips = new int[WIDTH];
		for(int i = 0; i<WIDTH; i++){
			skips[i] = 0;
		}
		
		for(int i = 0; i<WIDTH; i++){
			int sum = 0;
			for(int j = 0; j<numOfVariables; j++){
				String fileStr = "rlt/LSH-exp2/exp2/exp2-"+round+"/cluster-"+i+"-iloss-"+j;
				File file = new File(fileStr);
				if(file.exists()){
					sum++;
				}
			}
			skips[i] = sum;
		}
		
		return skips;
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
				String fileStr = "rlt/LSH-exp2/exp2/exp2-"+i+"/cluster-time";
				File file = new File(fileStr);
				if(!file.exists()){
					System.out.println("Error: "+fileStr+" does not exist!");
				}
				else{
					int totalSkips = 0;
					int[] skips = this.getSkips(i);
					for(int j = 0; j<widthOfBand; j++){
						totalSkips += skips[j];
					}
					
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
					String line=null;
					while(totalSkips > 0 ){
						line=br.readLine();
						totalSkips--;
					}
					if(line == null){
						System.out.println("Error: The line should not be null!");
					}
					int k = 0;
					while(k<skips[widthOfBand] && (line = br.readLine())!= null){
						tmpTime[i][k] = Double.valueOf(line.trim());
						k++;
					}
					
					br.close();
				}
			}
			
			for(int i = 0; i<numOfVariables; i++){
				DescriptiveStatistics stats = new DescriptiveStatistics();
				for (int j = 0; j<numOfRounds; j++){
					if(tmpTime[j][i] >= 0){
						stats.addValue(tmpTime[j][i]);
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
		// TODO Auto-generated method stub
		LSHExp2Time time2 = new LSHExp2Time(2);
		time2.dataReading();
		time2.displaystatistics();

	}

}
