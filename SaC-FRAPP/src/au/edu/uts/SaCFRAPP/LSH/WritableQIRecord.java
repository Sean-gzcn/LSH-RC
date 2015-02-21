package au.edu.uts.SaCFRAPP.LSH;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

import au.edu.uts.SaCFRAPP.common.Common;

public class WritableQIRecord implements Writable{

	Vector<String> categoricalQI;
	
	public WritableQIRecord(){
		categoricalQI = new Vector<String>();
	}
	
	public WritableQIRecord(String valueString){
		
		categoricalQI = new Vector<String>();
		Vector<String> rawValues = Common.stringToVector(valueString);
		for(int i = 0; i<Common.NUM_ATTR; i++){
			categoricalQI.add(rawValues.get(i).trim());
		}		
	}
	
	public String toString(){
		
		String string = "";
		for(int i = 0; i<categoricalQI.size()-1; i++){
			string += categoricalQI.get(i)+", ";
		}
		string += categoricalQI.get(categoricalQI.size()-1);
		
		return string;
	}
	
	public Vector<String> getCategoricalQI(){
		return categoricalQI;
	}
	
	public void setCategoricalQI(Vector<String> categoricalQI){
		this.categoricalQI = categoricalQI;
	}
	
	public static WritableQIRecord getAncestor(WritableQIRecord recordX, WritableQIRecord recordY){
		WritableQIRecord wr = new WritableQIRecord();
		Vector<String> categoricalQI = new Vector<String>();
		for(int i = 0; i<Common.NUM_ATTR; i++){
			categoricalQI.add(Common.taxonomyForest.getLowestCommonAncestor(i, 
					recordX.getCategoricalQI().get(i), recordY.getCategoricalQI().get(i)));
		}
		wr.setCategoricalQI(categoricalQI);		
		return wr;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {

		out.writeInt(categoricalQI.size());
		for(Iterator<String> i=categoricalQI.iterator(); i.hasNext();)
			out.writeUTF(i.next());
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		categoricalQI = new Vector<String>();
		int size = in.readInt();
		for(int i = 0; i<size; i++){
			categoricalQI.add(in.readUTF());
		}
	}
	
	public static void main(String args[]){
		
		WritableQIRecord recordx = new WritableQIRecord("45, 12th, Married-civ-spouse, Sales, Other-relative, White, Female, Italy, Private");
		WritableQIRecord recordy = new WritableQIRecord("82, Assoc-acdm, Married-AF-spouse, Transport-moving, Other-relative, Black, Female, Germany, SelfEmpNotInc");
		System.out.println("Record x: \n"+recordx.toString());
		System.out.println("Record y: \n"+recordy.toString());
		
		WritableQIRecord ancestor = WritableQIRecord.getAncestor(recordx, recordy);
		System.out.println("Ancestor: \n"+ancestor.toString());
	}
	
}
