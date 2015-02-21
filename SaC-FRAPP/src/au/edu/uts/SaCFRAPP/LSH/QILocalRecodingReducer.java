package au.edu.uts.SaCFRAPP.LSH;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import au.edu.uts.SaCFRAPP.common.Common;

public class QILocalRecodingReducer extends Reducer<Text, Text, Text, Text>{
	
	QIDistance dist;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		dist = new QIDistance();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		Vector<WritableQIRecord> records = new Vector<WritableQIRecord>();
		double iLoss = 0.0;
		
		Iterator<Text> itr = values.iterator();
		while(itr.hasNext()){
			records.add(new WritableQIRecord(itr.next().toString()));
		}
		
		// Generate anonymous quasi-identifier
		Vector<String> anonymousCluster = new Vector<String>();
		for(int i = 0; i<Common.NUM_ATTR; i++){
			String ancestor = records.get(0).getCategoricalQI().get(i);
			for(int j = 1; j<records.size(); j++){
				String tmpValue = records.get(j).getCategoricalQI().get(i);
				ancestor = Common.taxonomyForest.getLowestCommonAncestor(i, ancestor, tmpValue);
			}
			anonymousCluster.add(ancestor);
		}
		
		for(int i = 0; i<records.size(); i++){
			for(int j = 0; j<Common.NUM_ATTR; j++){
				String ancestor = anonymousCluster.get(j);
				iLoss += dist.getQIWeight(j)*
						(Common.taxonomyForest.getNumOfLeaves(j, ancestor)-1.0)/
						Common.taxonomyForest.getNumOfLeaves(j);
			}
		}
		anonymousCluster.add(String.valueOf(iLoss));
		
		context.write(new Text("*"+key.toString()), new Text(Common.vectorToString(anonymousCluster)));
	}
}
