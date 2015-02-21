package au.edu.uts.SaCFRAPP.LSH;

import java.util.BitSet;
import java.util.Random;

public class MinHash {
	
	Converter con;
	int recordLength;
	
	public MinHash(){
		con = new Converter();
		//recordLength = getPrimeNotLessThan(con.getRecordLength());
		recordLength = con.getRecordLength();
	}
	
	public int minHash(String record, int a, int b, int c){
		
		int result = Integer.MAX_VALUE;
		BitSet bs = con.convert(record);
		
		for(int i = 0; i<recordLength; i++){
			if(bs.get(i)){
				int hash = this.permutationHash(i, a, b, c, recordLength);
				if(hash < result){
					result = hash;
				}
			}
		}
		
		return result;
	}
	
	public int permutationHash(int x, int a, int b, int c, int length){
        //int hashValue = (int)((a * (x >> 4) + b * x + c) & 131071)%getPrimeNotLessThan(length);
		int hashValue = (a*x+b)%getPrimeNotLessThan(length); //True row permutations
        return Math.abs(hashValue)%length;
    }

	public static int getPrimeNotLessThan(int x){
		int y = x;
		if(y%2 == 0 && y != 2){
			y++;
		}
		while(!isPrime(y)){
			y += 2;
		}
		return y;
	}
	
	public static boolean isPrime(int x){
		if(x<2){
			System.err.println("Input Error!!!");
		}
		if(x == 2 || x== 3){
			return true;
		}
		if(x%2 == 0){
			return false;
		}
		if(x%3 == 0){
			return false;
		}
		
		int upperLimit = (int)Math.ceil(Math.sqrt(x));
		int i = 1;
		int divisor = 6*i-1;
		while (divisor <= upperLimit){
			if(x%divisor == 0){
				return false;
			}
			divisor += 2;
			if(x%divisor == 0){
				return false;
			}
			i++;
			divisor = 6*i-1;
		}
		return true;
	}
	
	public static void main(String[] arg){
		MinHash mh = new MinHash();
		
		Random r = new Random();
		int a = r.nextInt();
		int b = r.nextInt();
		int c = r.nextInt();
		
		String recordx = "39, Bachelors, Never-married, Adm-clerical, Not-in-family, White, Male, United-States, StateGov";
		//String recordy = "37, Masters, Married, Adm-clerical, Not-in-family, White, Female, United-States, StateGov";
		String recordy = "37, Bachelors, Never-married, Adm-clerical, Not-in-family, White, Male, United-States, StateGov";
		int minHashx = mh.minHash(recordx, a, b, c);
		int minHashy = mh.minHash(recordy, a, b, c);
		System.out.println("MinHash is: \n"+minHashx);
		System.out.println(minHashy);
		
		int count = 0;
		int COUNT = 10;
		for(int i = 0; i<COUNT; i++){
			a = r.nextInt();
			b = r.nextInt();
			c = r.nextInt();
			if(mh.minHash(recordx, a, b, c) == mh.minHash(recordy, a, b, c)){
				count++;
			}
		}
		System.out.println("Similarity is estimated as: "+(count*1.0/COUNT)*100+"%");
	}
}
