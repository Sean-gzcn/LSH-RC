package au.edu.uts.SaCFRAPP.common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class ValueTransformation extends TreeMap<String, String>{

	public ValueTransformation(){
		super();
	}
	
	public ValueTransformation(String dataPath){
		
		super();
		
		try{
			/**
	         * Modify to make it able to be exported in a jar file.
	         * */
			//BufferedReader br = new BufferedReader(new FileReader(dataPath));
			BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(dataPath)));
			
			String str = null;
			while((str = br.readLine())!= null){
			//System.out.println(str);
				StringTokenizer st = new StringTokenizer(str, ",");
				this.put(st.nextToken().trim(), st.nextToken().trim());
			}
			
			br.close();
		}
		catch(FileNotFoundException e){
			e.printStackTrace();
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]){
		ValueTransformation vt = new ValueTransformation("res/FormatTable.txt");
		System.out.println(vt.get("10th grade"));
	} 
}
