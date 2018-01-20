import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class MyPartitioner {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	   
	         try{
	            String[] str = value.toString().split(",");	 
	            String gender = str[3];
	            context.write(new Text(gender),new Text(value));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	      
	 }
	
	
	public static class PartClass extends Partitioner<Text,Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numProduceTask) {
		String[] str =value.toString().split(",");
		int age =Integer.parseInt(str[2]);
		
		if(age<=20)
		{
			return 0 % numProduceTask;
			
		}
		else if(age>20 && age<=30)
		{
			return 1 % numProduceTask;
		}
		else
		{
			return 2 % numProduceTask;
			
		}
		}
		
	}
	 public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	 {
		 public int max=0;
		 private Text outputkey=new Text();
		 
		 public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
		 {
			max=0;
			for(Text val:values)
			{
				String[] str =val.toString().split(",");
				if(Integer.parseInt(str[4])>max)
				{
					max=Integer.parseInt(str[4]);
					String mykey=str[3]+','+str[1]+','+str[2];
					outputkey.set(mykey);	
				}
			}
			 context.write(outputkey, new IntWritable(max));
		 }
	 }
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	 	{
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "partition");
		    job.setJarByClass(MyPartitioner.class);
		    FileInputFormat.setInputPaths(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    job.setMapperClass(MapClass.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setPartitionerClass(PartClass.class);
		    job.setReducerClass(ReduceClass.class);// without reducer 
		    job.setNumReduceTasks(3);
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

}