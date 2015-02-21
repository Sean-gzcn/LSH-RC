package au.edu.uts.SaCFRAPP.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class Pair implements Writable{

	private String generalization;
	private double info;
	private boolean validity;
//	private boolean anonymous;
	private double anonymity;
	
	public Pair(){
		generalization = null;
		info = -1;
		validity = true;
//		anonymous = false;
		anonymity = -1;
	}
	
	public Pair(String generalization){
		this.generalization = generalization;
		info = -1;
		validity = true;
//		anonymous = false;
		anonymity = -1;
	}
	
	public Pair(String generalization, double info){
		this.generalization = generalization;
		this.info = info;
		validity = true;
//		anonymous = false;
		anonymity = -1;
	} 
	
	public String getGeneralizaiton(){
		return generalization;
	}
	
	public void setGeneralization(String generalization){
		this.generalization = generalization;
	}
	
	public double getInfo(){
		return info;
	}
	
	public void setInfo(double info){
		this.info = info;
	}
	
	public void setValidity(boolean validity){
		this.validity = validity;
	}
	
	/*
	public boolean isValid(){
		return validity;
	}
	*/
	
	public boolean isValid(int K){
		return this.anonymity > K && this.validity;
	}
	
	/*
	public void setAnonymous(boolean anonymous){
		this.anonymous = anonymous;
	}
	*/
	/*
	public boolean isKAnonymized(){
		return anonymous;
	}
	*/
	
	public boolean isKAnonymized(int K){
		return anonymity>=K ? true : false;
	}
	
	public double getAnonymity(){
		return anonymity;
	}
	
	public void setAnonymity(double anonymity){
		this.anonymity = anonymity;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.generalization = in.readUTF();
		this.info = in.readDouble();
		this.validity = in.readBoolean();
//		this.anonymous = in.readBoolean();
		this.anonymity = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.generalization);
		out.writeDouble(this.info);	
		out.writeBoolean(validity);
//		out.writeBoolean(anonymous);
		out.writeDouble(this.anonymity);
	}
	
	@Override
	public String toString(){
		return "<"+this.generalization+", "+this.info+", "+this.validity+", "/*+this.anonymous+", "*/+anonymity+">";
	}
}
