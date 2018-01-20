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
public class CustProd4MonTot {

	public static class CustMap1 extends Mapper<LongWritable,Text,IntWritable,Text>{
	
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
		{
			IntWritable textKey=new IntWritable();
			Text textValue=new Text();
			int month1=0;
			long newsum=0;
			String[] str=value.toString().split(";");
			long amount=Long.parseLong(str[8]);
			newsum+=amount;
			String date1=str[0];
			String custid=str[1];
			
			Calendar cal=Calendar.getInstance();
			SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    	Date d1=null;
			try {
				d1 = sdf.parse(date1);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	cal.setTime(d1);
	    	month1=cal.get(Calendar.MONTH);
			
			String v=String.valueOf(newsum)+","+date1+","+custid;
			
			//String f=custid+","+date;
			textKey.set(month1);
			textValue.set(v);
			context.write(textKey, textValue);
		}
	
	}
	
	public static class RedMap1 extends Reducer<IntWritable,Text,Text ,LongWritable>
	{
		
		LongWritable result= new LongWritable();
		IntWritable all=new IntWritable();
		Text keyresult=new Text();
	    public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	    	long max_val=0;
	    	//long newsum=0;
	    	//int amt=0;
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
	job.setJarByClass(CustProd4MonTot.class);
	job.setMapperClass(CustMap1.class);
	job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
	job.setReducerClass(RedMap1.class);
	job.setNumReduceTasks(1);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
  
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);

}
}
//input files D01,D02,D11,D12
//input file directory on hdfs:niit_company_retail
//output-niit_company_output/4monthoutput13
//jan-01062489  	45554
//feb-01622362  	444000
//nov-02119083  	62688
//dec-02134819  	70589 // should be 2 values.... its overwriting 1st value

