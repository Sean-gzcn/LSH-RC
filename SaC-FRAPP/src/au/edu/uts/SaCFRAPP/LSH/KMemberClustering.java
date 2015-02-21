package au.edu.uts.SaCFRAPP.LSH;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Vector;

public abstract class KMemberClustering {

	int K = -1;
	QIDistance dist;
	
	public KMemberClustering(){
		dist = new QIDistance();
	}
	
	
	
	public void setK(int K){
		this.K = K;
	}
	
	public int getK(){
		return this.K;
	}
	
	public QIDistance getQIDistance(){
		return dist;
	}
	
	public Vector<WritableQIRecord> readDataRecords(String input){
		
		Vector<WritableQIRecord> records = new Vector<WritableQIRecord>();
		try{
			File file = new File(input);
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			
			String line;
			while((line = br.readLine()) != null){
				records.add(new WritableQIRecord(line.trim()));
			}
			
			br.close();
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
		
		return records;
	}
	
	public Vector<String> readDataStrings(String input){
		
		Vector<String> records = new Vector<String>();
		try{
			File file = new File(input);
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			
			String line;
			while((line = br.readLine()) != null){
				records.add(line.trim());
			}
			
			br.close();
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
		
		return records;
	}
	
	public void writeClusters(String output, Vector<Cluster> clusters){
		
		try{
			File file = new File(output);
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file)));
			
			for(int i = 0; i<clusters.size(); i++){
				Iterator<WritableQIRecord> wrItr = clusters.get(i).getClusterElements().iterator();
				while(wrItr.hasNext()){
					pw.append(i+"\t"+wrItr.next().toString()+"\n");
				}
			}
			pw.flush();
			pw.close();
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	protected abstract Vector<Cluster> kmClustering(String input);
	
	public void clustering(int K, String input, String output){
		this.setK(K);
		this.writeClusters(output, this.kmClustering(input));
	}
}
