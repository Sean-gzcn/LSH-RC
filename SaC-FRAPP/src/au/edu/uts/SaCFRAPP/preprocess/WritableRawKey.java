package au.edu.uts.SaCFRAPP.preprocess;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WritableRawKey implements WritableComparable<WritableRawKey>{

	String attribute;
	String value;
	
	public WritableRawKey(){
		attribute = null;
		value = null;
	}
	
	public WritableRawKey(String attribute, String value){
		this.attribute = attribute;
		this.value = value;
	}
	
	public String getAttribute(){
		return attribute;
	}
	
	public String getValue(){
		return value;
	}
	
	
	public String toString(){
		return "<"+attribute+", "+value+">"; 
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		attribute = in.readUTF();
		value = in.readUTF();
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(attribute);
		out.writeUTF(value);
	}

	@Override
	public int compareTo(WritableRawKey other) {
		
		int rlt = attribute.compareTo(other.getAttribute());
		if(rlt == 0){
			rlt = value.compareTo(other.getValue());
		}
		
		return rlt;
	}
	
	@Override
	public boolean equals(Object o){
		if (!(o instanceof WritableRawKey)) {
			return false;
		}

		WritableRawKey other = (WritableRawKey)o;
		return this.attribute.equals(other.getAttribute()) && this.value.equals(other.getValue());
	}
}
