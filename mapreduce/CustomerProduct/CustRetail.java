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
public class CustRetail {

	public static class CustMap extends Mapper<LongWritable,Text,Text,Text>{
	
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
		{
			Text textKey=new Text();
			Text textValue=new Text();
			
			long newsum=0;
			String[] str=value.toString().split(";");
			long amount=Long.parseLong(str[8]);
			newsum+=amount;
			String date=str[0];
			String custid=str[1];
			String v=String.valueOf(newsum)+","+date+","+custid;
			//String f=custid+","+date;
			textKey.set("all");
			textValue.set(v);
			context.write(textKey, textValue);
		}
	
	}
	
	public static class RedMap extends Reducer<Text,Text,Text ,LongWritable>
	{
		SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
		LongWritable result= new LongWritable();
		IntWritable all=new IntWritable();
		Text keyresult=new Text();
	    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	    	long max_val=0;
	    	//long sum=0;
	    	//int month1=0;
	    	String id1=null;
	    	for(Text v: values)
	    	{
	    	String[] val=v.toString().split(",");
	    	long amt =Long.parseLong(val[0]);
	    	
	    	
	    	if(amt>max_val)
	    	{
	    		max_val=amt;	
	    		id1=val[2];
	    	}
	    	}
	    	
//	    	Calendar cal=Calendar.getInstance();
//	    	for(Text d3: key)
//	    	{
//	    	String[] keyval=d3.toString().split(",");
//	    	Date d1=sdf.parse(keyval[1]);
//	    	cal.setTime(d1);
//	    	month1=cal.MONTH;
//	    	
//	    	}
	    	
	    	keyresult.set(id1);
	    	//all.set(month1);
	    	result.set(max_val);
	      context.write(keyresult, result);
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
	job.setJarByClass(CustRetail.class);
	job.setMapperClass(CustMap.class);
	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
	job.setReducerClass(RedMap.class);
	job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
  
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
