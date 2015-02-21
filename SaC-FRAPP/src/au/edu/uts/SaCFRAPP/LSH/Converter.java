package au.edu.uts.SaCFRAPP.LSH;

import au.edu.uts.SaCFRAPP.common.*;
import java.util.BitSet;
import java.util.Vector;

public class Converter {
	
	int numOfAttributes;
	Vector<Vector<Vector<String>>> mappingValues;
	int recordLength = 0;
	
	public Converter(){
		
		// Initialize the mapping
		numOfAttributes = Common.taxonomyForest.getNumberOfTrees();
		mappingValues = new Vector<Vector<Vector<String>>>();
		for(int i = 0; i<numOfAttributes; i++){
			Vector<Vector<String>> treeValues = new Vector<Vector<String>>();
			int levels = Common.taxonomyForest.getHeight(i);
			for(int j = 0; j<levels; j++){
				treeValues.add(Common.taxonomyForest.getDomainValuesByLevel(i, j));
				recordLength += treeValues.get(j).size();
			}
			mappingValues.add(treeValues);
		}
	}
	
	public void printMappingValues(){
		for(int i=0; i<mappingValues.size(); i++){
			Vector<Vector<String>> treeValues = mappingValues.get(i);
			for(int j=0; j<treeValues.size(); j++){
				System.out.println(treeValues.get(j));
			}
			System.out.println();
		}
	}
	
	public int getRecordLength(){
		return recordLength;
	}
	
	public BitSet convert(String record){
		BitSet bs = new BitSet();
		int base = 0;
		Vector<String> values = Common.stringToVector(record);
		for(int i=0; i<values.size()-1; i++){  // For Quasi-identifer attributes
			String value = values.get(i);
			Vector<Vector<String>> treeValues = mappingValues.get(i);
			for(int j = 0; j<treeValues.size(); j++){
				Vector<String> domainValues = treeValues.get(j);
				for(int k = 0; k<domainValues.size(); k++){
					if(value.equals(domainValues.get(k))){
						bs.set(base+k);
						break;
					}
				}
				value = Common.taxonomyForest.getParent(i, value);
				base += domainValues.size();
			}
		}
		return bs;
	}

	
	public static void main(String[] arg){
		
		//Common.taxonomyForest.printForest();
		Converter con = new Converter();
		con.printMappingValues();
		System.out.println("Length is: "+con.getRecordLength());
		
		String record = "39, Bachelors, Never-married, Adm-clerical, Not-in-family, White, Male, United-States, StateGov";
		BitSet bs = con.convert(record);
		System.out.println(bs);
		
		record = "43, Masters, Divorced, Exec-managerial, Unmarried, White, Female, United-States, SelfEmpNotInc";
		bs = con.convert(record);
		System.out.println(bs);
	} 
}
