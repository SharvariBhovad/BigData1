
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
public class CatProfAllMonth {

	public static class CustMap extends Mapper<LongWritable,Text,Text,IntWritable >{
	
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
		{
			Text textKey=new Text();
			IntWritable textValue=new IntWritable();
			
			int profit=0;
			String[] str=value.toString().split(";");
			int cost=Integer.parseInt(str[8]);
			int sales=Integer.parseInt(str[7]);
			profit=sales-cost;
			
			String prodid=str[4];
			
			
				textKey.set(prodid.toString());
				textValue.set(profit);
			context.write(textKey, textValue);
			
		}
	}
	
	public static class RedMap extends Reducer<Text,IntWritable ,Text ,IntWritable>
	{
		LongWritable result= new LongWritable();
		IntWritable all=new IntWritable();
		Text keyresult=new Text();
	    public void reduce(Text key, Iterable<IntWritable > values,Context context) throws IOException, InterruptedException {
	    	int max_val=0;
	    	for(IntWritable v: values)
	    	{
	    			max_val+=v.get();		
	    	}
	    	all.set(max_val);
			context.write(key, all);
	    }
	    
	}
	
	
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
{
	if (args.length != 2) {
		System.out
				.printf("Usage: StockPercentChangeDriver <input dir> <output dir>\n");
		System.exit(-1);
	}

	Configuration conf= new Configuration();
	Job job = Job.getInstance(conf,"Cutomer_Retail");
	job.setJarByClass(CatProfAllMonth.class);
	job.setMapperClass(CustMap.class);
	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(RedMap.class);
	job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
  
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
