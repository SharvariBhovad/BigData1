import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class SpeedDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		if(args.length!=2)
		{
			System.out.printf("Usage: StockPercentChangeDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"Speed_computation");
		job.setJarByClass(SpeedDriver.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(SpeedMapper.class);
		job.setReducerClass(SpeedReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}
