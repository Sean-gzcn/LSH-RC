package au.edu.uts.SaCFRAPP.DataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class LSHExp1Iloss extends ExperimentStatistics {
	
	public LSHExp1Iloss(){
		super(10, 10);
	}
	
	@Override
	public void dataReading(){
		
		try{
			for(int i = 0; i<numOfVariables; i++){
				DescriptiveStatistics stats = new DescriptiveStatistics();
				for(int j = 0; j<numOfRounds; j++){
					String fileStr = "rlt/LSH-exp1/exp1/completed_0_"+j+"/output/agglom/cluster_1000-iloss-"+i;
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
						StringTokenizer st = new StringTokenizer(line);
						st.nextToken();
						stats.addValue(Double.valueOf(st.nextToken().trim())/1000);
						
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
	
	public void displayGreedy(){
		try{
			DescriptiveStatistics stats = new DescriptiveStatistics();
			for(int j = 0; j<numOfRounds; j++){
				String fileStr = "rlt/LSH-exp1/exp1/completed_0_"+j+"/output/greedy/cluster_1000-iloss-0";
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
					StringTokenizer st = new StringTokenizer(line);
					st.nextToken();
					stats.addValue(Double.valueOf(st.nextToken().trim())/1000);
					
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
		// TODO Auto-generated method stub
		LSHExp1Iloss exp1 = new LSHExp1Iloss();
		exp1.dataReading();
		exp1.displaystatistics();
		exp1.displayGreedy();

	}

}
