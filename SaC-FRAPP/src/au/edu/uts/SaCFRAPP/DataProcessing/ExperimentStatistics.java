package au.edu.uts.SaCFRAPP.DataProcessing;

public abstract class ExperimentStatistics {

	int numOfRounds;
	int numOfVariables;
	long[] counts;
	double[] means;
	double[] sDeviations;
	double[] sErrors;
	
	public ExperimentStatistics(int numOfRounds, int numOfVariables){
		this.numOfRounds = numOfRounds;
		this.numOfVariables = numOfVariables;
		counts = new long[numOfVariables];
		means = new double[numOfVariables];
		sDeviations = new double[numOfVariables];
		sErrors = new double[numOfVariables];
	}
	
	public abstract void dataReading();
	
	public void displaystatistics(){
		for(int i = 0; i<numOfVariables; i++){
			System.out.println(i+"\t"+counts[i]+"\t"+means[i]+"\t"+sDeviations[i]+"\t"+sErrors[i]);
		}
	}
	
	public int getNumOfRounds(){
		return this.numOfRounds;
	}
	
	public int getNumOfVariables(){
		return this.numOfVariables;
	}
	
	public long[] getCounts(){
		return this.counts;
	}
	
	public double[] getMeans(){
		return this.means;
	}
	
	public double[] getStandardDeviations(){
		return this.sDeviations;
	}
	
	public double[] getStandardErrors(){
		return this.sErrors;
	}
}
