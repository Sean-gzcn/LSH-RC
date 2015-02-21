package au.edu.uts.SaCFRAPP.DataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class LSHExp2Iloss extends ExperimentStatistics {

	int widthOfBand;
	
	public LSHExp2Iloss(int widthOfBand) {
		super(10, 10);
		
		this.widthOfBand = widthOfBand;
	}

	@Override
	public void dataReading() {
		try{
			for(int i = 0; i<numOfVariables; i++){
				DescriptiveStatistics stats = new DescriptiveStatistics();
				for(int j = 0; j<numOfRounds; j++){
					String fileStr = "rlt/LSH-exp2/exp2/exp2-"+j+"/cluster-"+widthOfBand+"-iloss-"+i;
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
						stats.addValue(Double.valueOf(st.nextToken().trim())/((i+1)*10000));
						
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
		LSHExp2Iloss loss2 = new LSHExp2Iloss(3);
		loss2.dataReading();
		loss2.displaystatistics();

	}

}
