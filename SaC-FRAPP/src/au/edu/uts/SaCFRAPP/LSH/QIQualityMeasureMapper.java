package au.edu.uts.SaCFRAPP.LSH;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.edu.uts.SaCFRAPP.common.Common;


public class QIQualityMeasureMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
		itr.nextToken();
		Vector<String> properties = Common.stringToVector(itr.nextToken().trim());
		
		context.write(new Text("ILOSS"), new DoubleWritable(Double.valueOf(properties.get(Common.NUM_ATTR))));
	}
}
