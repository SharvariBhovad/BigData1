import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
//import org.apache.hadoop.io.DoubleWritable.

public class AgeGroupViableProduct {
public static class Mapper1 extends Mapper<LongWritable,Text,Text,LongWritable>{
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		String[] line=value.toString().split(";");
		String prod_id=line[5];
		long cost=Integer.parseInt(line[7]);
		long sales=Integer.parseInt(line[8]);
		String age=line[2];
		
		long profit=sales-cost;
		
		String keyvl=prod_id+";"+age;
		context.write(new Text(keyvl), new LongWritable(profit));
	}
}


	public static class Reducer1 extends Reducer<Text,LongWritable,LongWritable,Text>
	{
		
		public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException,InterruptedException
		{
			long totprofit=0;
			for(LongWritable val: values)
			{
			long profit=val.get();
			totprofit+=profit;
			}
			if(totprofit>0)
			{
				context.write(new LongWritable(totprofit),new Text(key));
			}
			
		}
	}
		
		public static class SortMapper extends Mapper<LongWritable,Text,LongWritable,Text>
		{
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
				String[] newline=value.toString().split("\t");
				long prof=Long.parseLong((newline[0]));
				String k=newline[1];
				context.write(new LongWritable(prof), new Text(k));
			}
		}
		public static class Part1 extends Partitioner<LongWritable,Text>
		{
			@Override
			public int getPartition(LongWritable key,Text value, int numProduceTask) {
				String[] newLine=value.toString().split(";");
				String age=newLine[1];
				if(age.equals("A "))
				{
					return 0 % numProduceTask;
				}
				else if(age.equals("B "))
				{
					return 1 % numProduceTask;
				}
				else if(age.equals("C "))
				{
					return 2 % numProduceTask;
				}
				else if(age.equals("D "))
				{
					return 3 % numProduceTask;
				}
				else if(age.equals("E "))
				{
					return 4 % numProduceTask;
				}
				else if(age.equals("F "))
				{
					return 5 % numProduceTask;
				}
				else if(age.equals("G "))
				{
					return 6 % numProduceTask;
				}
				else if(age.equals("H "))
				{
					return 7 % numProduceTask;
				}
				else if(age.equals("I "))
				{
					return 8 % numProduceTask;
				}
				else if(age.equals("J "))
				{
					return 9 % numProduceTask;
				}
				else
				{
					return 10 % numProduceTask;
				}
				}
			
		}
		
		public static class SortReducer extends Reducer<LongWritable,Text,Text,LongWritable>
		{
			int counter=1;
			public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
			{
				for(Text val : value)
				{
					if(counter<=5)
					{			
//						context.write(new Text(val),new IntWritable( prof));	
						context.write(val, key);
					}	
				}
				counter++;
			}
		}
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
		 	{
			    Configuration conf = new Configuration();
			   
			    Job job = Job.getInstance(conf, "partition");
			    job.setJarByClass(AgeGroupViableProduct.class);
			  //  FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    job.setMapperClass(Mapper1.class);
			    job.setMapOutputKeyClass(Text.class);
			    job.setMapOutputValueClass(LongWritable.class);
			    
			    job.setReducerClass(Reducer1.class);// without reducer 
			    job.setNumReduceTasks(1);
			  
			    job.setOutputKeyClass(LongWritable.class);
			    job.setOutputValueClass(Text.class);
			    FileInputFormat.setInputPaths(job, new Path(args[0]));
			   // FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    Path outputPath3 = new Path("FirstMapper1");
				FileOutputFormat.setOutputPath(job, outputPath3);
				FileSystem.get(conf).delete(outputPath3, true);
				job.setInputFormatClass(TextInputFormat.class);
			    job.setOutputFormatClass(TextOutputFormat.class);
				//job.waitForCompletion(true);
			    
				
			    Job job2 = Job.getInstance(conf, "partition1");
			    job2.setJarByClass(AgeGroupViableProduct.class);
			    
			    job2.setMapperClass(SortMapper.class);
			    job2.setMapOutputKeyClass(LongWritable.class);
			    job2.setMapOutputValueClass(Text.class);
			    
			   
			    job2.setPartitionerClass(Part1.class);
			    job2.setSortComparatorClass(DecreasingComparator.class);
			    job2.setReducerClass(SortReducer.class);
			    job2.setNumReduceTasks(11);
			   
			  
			    job2.setInputFormatClass(TextInputFormat.class);
			    job2.setOutputFormatClass(TextOutputFormat.class);
			    
			    job2.setOutputKeyClass(Text.class);
			    job2.setOutputValueClass(LongWritable.class);
			    
			    FileInputFormat.setInputPaths(job2, outputPath3);
			    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			    FileSystem.get(conf).delete(new Path(args[1]), true);
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
}