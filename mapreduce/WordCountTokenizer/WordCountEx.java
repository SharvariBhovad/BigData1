import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCountEx {
	public static class WordMap extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		private final static IntWritable one =new IntWritable(1);
		private Text word= new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr =new StringTokenizer(value.toString());
			
			while(itr.hasMoreTokens()){
				String myword=itr.nextToken().toLowerCase();
				
				word.set(myword);
				context.write(word,one);
			}
			
		}
	}
	public static class WordRed extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result=new IntWritable();
	
		public void Reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable val : values)
			{
				sum+=val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
		{
			if(args.length!=2)
			{
				System.out.printf("Usage: StockPercentChangeDriver <input dir> <output dir>\n");
				System.exit(-1);
			}
			Configuration conf =new Configuration();
			Job job=Job.getInstance(conf,"Speed_computation");
			job.setJarByClass(WordCountEx.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setMapperClass(WordMap.class);
			job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    
			job.setReducerClass(WordRed.class);
			
			job.setNumReduceTasks(1);
			
			job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			
			
			job.setOutputValueClass(DoubleWritable.class);
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
			
		}
}
