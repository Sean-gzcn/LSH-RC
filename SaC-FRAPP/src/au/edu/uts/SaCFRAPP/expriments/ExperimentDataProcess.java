package au.edu.uts.SaCFRAPP.expriments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class ExperimentDataProcess {
	
	int numOfRepetions = 5;
	int numOfValues = 6;
	int numOfBins = 20;
	int numOfNewBins = 10;
	String dataFolderPathStr = "dat/exp1/r=10000";
	
	int numOfTimeReptitions = 5;
	int numOfRounds = 10;
	String timeDataPathStr = "dat/exp2";
	
	double[] avg_avgE;
	double[] avg_DM;
	double[] avg_iLoss;
	double[] avg_MD;
	double[][] avg_bins;
	double[][] new_bins;
	Pair[][] avg_pBins;
	Pair[][] uni_pBins;
	
	double[] avg_Time;
	
	public class Pair{
		public double x;
		public double y;
		
		public Pair(){
			x = 0.0;
			y = 0.0;
		}
		
		public Pair(double x, double y){
			this.x = x;
			this.y = y;
		}
		
		void setX(double x){
			this.x = x;
		} 
		
		void setY(double y){
			this.y = y;
		}
		
		double getX(){
			return x;
		}
		
		double getY(){
			return y;
		}
		
		@Override
		public String toString(){
			return "<"+x+", "+y+">";
		}
	}

	public ExperimentDataProcess(){
		
		
		double[][] avgE = new double[numOfRepetions][numOfValues];
		double[][] DM = new double[numOfRepetions][numOfValues];
		double[][] iLoss = new double[numOfRepetions][numOfValues];
		double[][] MD = new double[numOfRepetions][numOfValues];
		double[][][] bins = new double[numOfRepetions][numOfValues][numOfBins];
		Pair[][][] pBins = new Pair[numOfRepetions][numOfValues][numOfBins];  // In order to store the values of each bin
		
		try{
			for(int i = 0; i<numOfRepetions; i++){
				for(int j = 0; j<numOfValues; j++){
					String inputPathStr = dataFolderPathStr+"/Repetion"+i+"/w="+j;
					File file = new File(inputPathStr);
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
					
					StringTokenizer itr = new StringTokenizer(br.readLine(), "\t");
					itr.nextToken();
					avgE[i][j] = Double.valueOf(itr.nextToken().trim());
					
					itr = new StringTokenizer(br.readLine(), "\t");
					itr.nextToken();
					DM[i][j] = Double.valueOf(itr.nextToken().trim());
					
					itr = new StringTokenizer(br.readLine(), "\t");
					itr.nextToken();
					iLoss[i][j] = Double.valueOf(itr.nextToken().trim());
					
					itr = new StringTokenizer(br.readLine(), "\t");
					itr.nextToken();
					MD[i][j] = Double.valueOf(itr.nextToken().trim());
					
					for(int k = 0; k<numOfBins; k++){
						itr = new StringTokenizer(br.readLine(), "\t");
						
						String binString = itr.nextToken();
						StringTokenizer binItr = new StringTokenizer(binString, "(");
						binItr.nextToken();
						String binValueStr = binItr.nextToken();
						double binValue = Double.valueOf(binValueStr.replace(')', '0').trim());
						bins[i][j][k] = Double.valueOf(itr.nextToken().trim());
						
						pBins[i][j][k] = new Pair(binValue, bins[i][j][k]);
					}
					
					br.close();
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		avg_avgE = this.average(avgE, numOfRepetions, numOfValues);
		avg_DM = this.average(DM, numOfRepetions, numOfValues);
		avg_iLoss = this.average(iLoss, numOfRepetions, numOfValues);
		avg_MD = this.average(MD, numOfRepetions, numOfValues);
		avg_bins = this.average(bins, numOfRepetions, numOfValues, numOfBins);
		new_bins = this.getNewBins(avg_bins, numOfValues, numOfBins);
		avg_pBins = this.average(pBins, numOfRepetions, numOfValues, numOfBins);
		uni_pBins = this.uniform(avg_pBins);
		
		double time[][] = new double[numOfTimeReptitions][numOfRounds];
		try{
			for(int i = 0; i<numOfTimeReptitions; i++){
				String inputPathStr = timeDataPathStr+"/Repetition"+i+".txt";
				File file = new File(inputPathStr);
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
				for(int j = 0; j<numOfRounds; j++){
					time[i][j] = Double.valueOf(br.readLine().trim());
				}
				br.close();
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		avg_Time = this.average(time, numOfTimeReptitions, numOfRounds);
	}
	
	public double[] getTime(){
		return avg_Time;
	}
	
	public double[] getAvgE(){
		return avg_avgE;
	}
	
	public double[] getDM(){
		return avg_DM;
	}
	
	public double[] getILoss(){
		return avg_iLoss;
	}
	
	public double[] getMD(){
		return avg_MD;
	}
	
	public double[][] getBins(){
		return avg_bins;
	}
	
	public double[][] getNewBins(){
		return new_bins;
	}
	
	private double[][] getNewBins(double[][] data, int I, int J){
		double[][] newBins = new double[I][numOfNewBins];
		for(int i = 0; i<I; i++){
			for(int j = 0; j<numOfNewBins; j++){
				newBins[i][j] = data[i][2*j]+data[i][2*j+1];
			}
		}
		return newBins;
	}
	
	private double[] average(double[][] data, int I, int J){
		double[] result = new double[J];
		for(int j = 0; j<J; j++){
			result[j] = 0.0;
		}
		
		for(int i=0; i<I; i++){
			for(int j = 0; j<J; j++){
				result[j] += data[i][j]; 
			}
		}
		
		for(int j = 0; j<J; j++){
			result[j] /= I;
		}
		
		return result;
	}
	
	private double[][] average(double[][][] data, int I, int J, int K){
		double result[][] = new double[J][K];
		for(int j = 0; j<J; j++){
			for(int k = 0; k<K; k++){
				result[j][k] = 0.0;
			}
		}
		
		for(int i=0; i<I; i++){
			for(int j = 0; j<J; j++){
				for(int k = 0; k<K; k++){
					result[j][k] += data[i][j][k];
				}
			}
		}
		
		for(int j = 0; j<J; j++){
			for(int k = 0; k<K; k++){
				result[j][k] /= I;
			}
		}
		
		return result;
	}
	
	private Pair[][] average(Pair[][][] data, int I, int J, int K){
		Pair result[][] = new Pair[J][K];
		for(int j = 0; j<J; j++){
			for(int k = 0; k<K; k++){
				result[j][k] = new Pair();
			}
		}
		
		for(int i=0; i<I; i++){
			for(int j = 0; j<J; j++){
				for(int k = 0; k<K; k++){
					result[j][k].setX(result[j][k].getX()+data[i][j][k].getX());
				}
			}
		}
		
		for(int j = 0; j<J; j++){
			for(int k = 0; k<K; k++){
				result[j][k].setX(result[j][k].getX()/I);
				result[j][k].setY(avg_bins[j][k]);
			}
		}
		
		return result;
	}
	
	private Pair[][] uniform(Pair[][] data){
		double[] steps = new double[numOfValues];
		for(int i = 0; i<numOfValues; i++){
			steps[i] = data[i][1].getX();
		}
		double maxStep = Double.MIN_VALUE;
		for(int i = 0; i<numOfValues; i++){
			if(steps[i] > maxStep){
				maxStep = steps[i];
			}
		}
		
		Pair[][] result = new Pair[numOfValues][numOfBins];
		for(int i = 0; i<numOfValues; i++){
			for(int j = 0; j<numOfBins; j++){
				result[i][j] = new Pair();
			}
		}
		
		for(int i = 0; i<numOfValues; i++){
			for(int j = 0; j<numOfBins; j++){
				
				result[i][j].setX(maxStep*(j+1));
				double lowerLimit = maxStep*j;
				double upperLimit = maxStep*(j+1);
				double minValue = -1;
				int minIndex = -1;
				for(int k = 0; k<numOfBins; k++){
					if(data[i][k].getX() > lowerLimit){
						minValue = data[i][k].getX();
						minIndex = k-1;
						break;
					}
				}
				double maxValue = -1;
				int maxIndex = -1;
				for(int k = 0; k<numOfBins; k++){
					if(data[i][k].getX() > upperLimit){
						maxValue = data[i][k].getX();
						maxIndex = k-1;
						break;
					}
				}
				
				double sum = 0.0;
				if(minIndex != -1){
					sum += (minValue-lowerLimit)/steps[i]*data[i][minIndex].getY();
					if(maxIndex != -1){
						for(int k = minIndex+1; k<maxIndex; k++){
							sum += data[i][k].getY();
						}
						sum += (1-(maxValue-upperLimit)/steps[i])*data[i][maxIndex].getY();
						
					}
					else{
						for(int k = minIndex+1; k<numOfBins; k++){
							sum += data[i][k].getY();
						}
					}
					result[i][j].setY(sum);
				}
				else{
					result[i][j].setY(0.0);
				}
			}
		}
		
		return result;
	}
	
	
	private void print(String name, double[] data){
		System.out.println(name+": ");
		for(int i = 0; i<data.length; i++){
			System.out.println(data[i]);
		}
		System.out.println();
	}
	
	private void printBins(double[][] data){
		System.out.println("Bins: ");
		for(int i=0; i<numOfBins; i++){
			for(int j = 0; j<numOfValues-1; j++){
				System.out.print(data[j][i]+"\t");
			}
			System.out.print(data[numOfValues-1][i]+"\n");
		}
		System.out.println();
	}
	
	private void printPairBins(Pair[][] data){
		System.out.println("PairBins: ");
		for(int i=0; i<numOfBins; i++){
			for(int j = 0; j<numOfValues-1; j++){
				System.out.print(data[j][i]+"\t");
			}
			System.out.print(data[numOfValues-1][i]+"\n");
		}
		System.out.println();
	}
	
	private void printUniBins(Pair[][] data){
		System.out.println("Uniformed Bins: ");
		for(int i=0; i<numOfBins; i++){
			System.out.print(data[0][i].getX()+"\t");
			for(int j = 0; j<numOfValues-1; j++){
				System.out.print(data[j][i].getY()+"\t");
			}
			System.out.print(data[numOfValues-1][i].getY()+"\n");
		}
		System.out.println();
	}
	
	private void printNewBins(double[][] data){
		System.out.println("New bins: ");
		for(int i=0; i<numOfNewBins; i++){
			for(int j = 0; j<numOfValues-1; j++){
				System.out.print(data[j][i]+"\t");
			}
			System.out.print(data[numOfValues-1][i]+"\n");
		}
		System.out.println();
	}
	
	public void print(){
		print("avgE", avg_avgE);
		print("DM", avg_DM);
		print("ILOSS", avg_iLoss);
		print("MD", avg_MD);
		printBins(avg_bins);
		printNewBins(new_bins);
		
		printPairBins(avg_pBins);
		printPairBins(uni_pBins);
		printUniBins(uni_pBins);
		
		print("Time", avg_Time);
	}
	
	public static void main(String[] args){
		
		ExperimentDataProcess edp = new ExperimentDataProcess();
		edp.print();		
	}
}


