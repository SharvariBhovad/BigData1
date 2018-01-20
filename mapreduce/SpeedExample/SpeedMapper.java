import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class SpeedMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] record=value.toString().split(",");
		String stockSymbol=record[0];
		double speedValue=Double.valueOf(record[1]);
		
		if(speedValue>65)
		{
	
		context.write(new Text(stockSymbol), new DoubleWritable(1));
		}
		else{
			context.write(new Text(stockSymbol), new DoubleWritable(0));

		}
	
	}

}
