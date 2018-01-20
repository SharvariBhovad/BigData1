import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
public static class Mapper1 extends Mapper<LongWritable,Text,Text,FloatWritable>{
	private final static FloatWritable one =new FloatWritable(1);
	Text text=new Text();
	public void map(LongWritable key,Text values, Context context) throws IOException, InterruptedException{
		String line=values.toString();
		for(String val: line.split("\\W+"))
		{
			if(val.length()>0)
			{
				text.set(val);
				context.write(text,one);
			}
		}
	}
}
public static class Reducer1 extends Reducer<Text,FloatWritable,Text,FloatWritable>
{
	FloatWritable con=new FloatWritable();
public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException
{
	float count=0.0f;
	for(FloatWritable val: value)
	{
		count+=val.get();
	}
	con.set(count);
	context.write(key,con);
}
}

public static class Mapper2 extends Mapper< LongWritable, Text, FloatWritable, Text> {

	  FloatWritable frequency = new FloatWritable();
	  
	  public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
		  String[] line= value.toString().split("\t");
		  float newVal=Float.parseFloat(line[1]);
	   // int newVal = Integer.parseInt(value.toString());
	    frequency.set(newVal);
	    context.write(frequency, new Text(line[0]));
	  }
	}


public static class Reducer22 extends Reducer<FloatWritable, Text, Text,FloatWritable> // swapping 
{
	public void reduce(FloatWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		for(Text vl : values)
		{
		context.write(new Text(vl),key);
		}
	}
}


public static void main(String[] args) throws Exception{
	Configuration conf=new Configuration();
	Job job1 =Job.getInstance(conf,"value-1");
	job1.setJarByClass(WordCount.class);
	job1.setMapperClass(Mapper1.class);
	job1.setReducerClass(Reducer1.class);
	
	job1.setMapOutputKeyClass(Text.class);
	job1.setMapOutputValueClass(FloatWritable.class);
	
	job1.setInputFormatClass(TextInputFormat.class);
	job1.setOutputFormatClass(TextOutputFormat.class);
	
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(FloatWritable.class);
	
	FileInputFormat.setInputPaths(job1,new Path(args[0]));
	//FileOutputFormat.setOutputPath(job1,new Path(args[1]));
	
	Path OutputPath1 =new Path("FirstMapper1");
	FileOutputFormat.setOutputPath(job1, OutputPath1);
	FileSystem.get(conf).delete(OutputPath1,true);
	job1.waitForCompletion(true);
	
	// ----------------------job2--------------------------
	
	Job job2 =Job.getInstance(conf,"total-value");
	job2.setJarByClass(WordCount.class);
	job2.setMapperClass(Mapper2.class);
	job2.setReducerClass(Reducer22.class);
job2.setSortComparatorClass(DecreasingComparator.class);
	job2.setMapOutputKeyClass(FloatWritable.class);
	job2.setMapOutputValueClass(Text.class);
	

	job2.setInputFormatClass(TextInputFormat.class);
	job2.setOutputFormatClass(TextOutputFormat.class);
	
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(FloatWritable.class);
	job2.setNumReduceTasks(1);
	

	FileInputFormat.addInputPath(job2, OutputPath1);
	//FileOutputFormat.setOutputPath(job2, OutputPath2);
	FileOutputFormat.setOutputPath(job2,new Path(args[1]));
	FileSystem.get(conf).delete(new Path(args[1]),true);
	System.exit(job2.waitForCompletion(true)? 0:1);
}
}
