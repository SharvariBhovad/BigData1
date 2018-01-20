import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SpeedReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
	DoubleWritable offenseValue= new DoubleWritable();


public void reduce(Text Key,Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
	double sum = 0;
	int totalcount=0;
	for(DoubleWritable value:values)
	{
		totalcount++;
		sum+=value.get();
				
		
	}
	double offencePercent=((sum/totalcount)*100);

	offenseValue.set(offencePercent);
	context.write(Key, offenseValue);
}
}
