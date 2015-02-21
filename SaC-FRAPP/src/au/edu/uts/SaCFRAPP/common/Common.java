package au.edu.uts.SaCFRAPP.common;
import au.edu.uts.SaCFRAPP.taxonomy.*;

import java.util.StringTokenizer;
import java.util.Vector;

public class Common {

	//public static TaxonomyForest taxonomyForest = new TaxonomyForest();
	//public static TaxonomyForest taxonomyForest = new TaxonomyForest("res/AttributeTaxonomy.xml");
	public static TaxonomyForest taxonomyForest = new TaxonomyForest("res/AttributeTaxonomyTrees.xml");
	
	public static int NUM_ATTR = 8;
	public static int NUM_SA = 7;
	public static int INDEX_SEN = 1;
	public static String[] SEN_VALUE = {"Private", "SelfEmpNotInc", "SelfEmpInc", "FederalGov", "LocalGov", "StateGov", "WithoutPay"};
	public static String[] TOP_VALUE = {"Any_Age", "Any_Education", "Any_Status", "Any_Job", "Any_Relationship", "Any_Race", "Any_Sex", "Any_Country"};
	public static Orderability[] ATTR_ORDERABILITY = {Orderability.Full, Orderability.Partial, Orderability.Partial, Orderability.Partial, Orderability.Partial, Orderability.Partial, Orderability.Partial, Orderability.Partial};
	public static AttributeNode [] ATTRIBUTES = new AttributeNode[NUM_ATTR];
	public static final int MAXAGE = 130;
	
	public static ValueTransformation vtMap = new ValueTransformation("res/FormatTable.txt");
	
	public Common(){
		
	}
	
	public static AttributeNode [] getAttributes(){
		
		// Setting attribute properties
		ATTRIBUTES[0] = new AttributeNode<Integer>(0);
		ATTRIBUTES[0].setLowerLimit(0);
		ATTRIBUTES[0].setUpperLimit(MAXAGE);
		for(int i = 1; i<NUM_ATTR; i++){
			ATTRIBUTES[i] = new AttributeNode(i);
		}
		for(int i = 0; i<NUM_ATTR; i++){
			ATTRIBUTES[i].setTopValue(TOP_VALUE[i]);
			ATTRIBUTES[i].setOrderability(ATTR_ORDERABILITY[i]);
		}
		
		return ATTRIBUTES;
	}
	
	public static String vectorToString(Vector<String> vector){
		String res = "";
		for(int i=0; i<vector.size()-1; i++){
			res += vector.elementAt(i)+", ";
		}
		res += vector.elementAt(vector.size()-1);
		return res;
	}
	
	public static Vector<String> stringToVector(String string){
		Vector<String> strVector = new Vector<String>();
		StringTokenizer itr = new StringTokenizer(string, ",");
		while(itr.hasMoreTokens()){
			strVector.add(itr.nextToken().trim());
		}
		return strVector;
	}
	
	public static String getQIString(String record){
		Vector<String> strVector = stringToVector(record);
		String res = "";
		for(int i = 0; i<strVector.size()-2; i++){
			res += strVector.get(i)+", ";
		}
		res += strVector.get(strVector.size()-2);
		return res;
	}
	
	public static Vector<String> rawStrToVector(String string){
		Vector<String> strVector = new Vector<String>();
		StringTokenizer itr = new StringTokenizer(string, ",.");
		while(itr.hasMoreTokens()){
			strVector.add(itr.nextToken().trim());
		}
		return strVector;
	}
	
	public static Vector<Double> arrayToVector(double[] array){
		Vector<Double> vec = new Vector<Double>();
		for(int i = 0; i<array.length; i++){
			vec.add(array[i]);
		}
		
		return vec;
	}
	
	public static Vector<String> arrayToVector(String[] array){
		Vector<String> vec = new Vector<String>();
		for(int i = 0; i<array.length; i++){
			vec.add(array[i]);
		}
		
		return vec;
	}
	
	public static void main(String args[]){
		//String string = "38, Private, 6, 36, 1st 2nd 3rd or 4th grade, 0, Not in universe, Married-civilian spouse present, Manufacturing-durable goods, Machine operators assmblrs & inspctrs, White, Mexican (Mexicano), Female, Not in universe, Not in universe, Full-time schedules, 0, 0, 0, Joint one under 65 & one 65+, Not in universe, Not in universe, Spouse of householder, Spouse of householder, 1032.38, ?, ?, ?, Not in universe under 1 year old, ?, 4, Not in universe, Mexico, Mexico, Mexico, Foreign born- Not a citizen of U S , 0, Not in universe, 2, 12, 95, - 50000.";
		String string = "2, Not in universe, 0, 0, Children, 0, Not in universe, Never married, Not in universe or children, Not in universe, White, Mexican-American, Male, Not in universe, Not in universe, Children or Armed Forces, 0, 0, 0, Nonfiler, Not in universe, Not in universe, Child <18 never marr not in subfamily, Child under 18 never married, 1601.75, ?, ?, ?, Not in universe under 1 year old, ?, 0, Both parents present, United-States, United-States, United-States, Native- Born in the United States, 0, Not in universe, 0, 0, 95, - 50000.";
		Vector<String> strVector = Common.rawStrToVector(string);
		System.out.println(strVector.toString());
		System.out.println("size is: "+strVector.size());
	}
}
