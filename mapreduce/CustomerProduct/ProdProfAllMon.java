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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
public class ProdProfAllMon {

	public static class CustMap extends Mapper<LongWritable,Text,Text,IntWritable >{
	
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
		{
			Text textKey=new Text();
			IntWritable textValue=new IntWritable();
			
			int profit=0;
			int percentage=0;
			String[] str=value.toString().split(";");
			int cost=Integer.parseInt(str[8]);
			int sales=Integer.parseInt(str[7]);
			profit=cost-sales;
			percentage=profit*100/cost;
			String prodid=str[5];
			textKey.set(prodid.toString());
			textValue.set(percentage);// for percentage niit_company_output/prodpercent28
			//textValue.set(profit);// for profit
//			String date=str[0];
//			String custid=str[1];
			//String v=String.valueOf(newsum)+","+date+","+custid;
			//String f=custid+","+date;

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
	    	int profamt=0;
	    	//String id1=null;
	    	
	    	for(IntWritable v: values)
	    	{
	    		max_val+=v.get();
	    		if(max_val>0)
	    		{
	    			profamt=max_val;
	    		}
	    		else{
	    			profamt=0;
	    		}
	    	}

	    	//keyresult.set(id1);
	    	all.set(profamt);
	    	///result.set(max_val);
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
	job.setJarByClass(ProdProfAllMon.class);
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
