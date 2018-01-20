import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
public class ReduceJoin {

	public static class CustMapper extends Mapper<LongWritable,Text,Text,Text> {
		public void map(LongWritable key, Text values, Context context) throws InterruptedException, IOException{
			String[] line=values.toString().split(",");
			context.write(new Text(line[0]), new Text("cust\t"+line[1]));
		}
	}
	
	public static class TranMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text values, Context context ) throws InterruptedException,IOException
		{
			String[] line=values.toString().split(",");
			context.write(new Text(line[2]), new Text("tran\t"+line[3]));
		}
	}
	
	public static class finalReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int count=0;
			double total=0.0;
			String name="";
			for(Text val : values)
			{
				String[] newLine=val.toString().split("\t");
				if(newLine[0].equals("tran")){
					count++;
					total+=Float.parseFloat(newLine[1]);
				}else if(newLine[0].equals("cust")){
					name=newLine[1];
				}
				
			}
			String str=String.format("%d\t%f", count,total);
			context.write(new Text(name), new Text(str));		
			}
	}
	public static void main(String[] args) throws Exception{
		Configuration config= new Configuration();
		Job job=Job.getInstance(config);
		job.setJarByClass(ReduceJoin.class);
		job.setJobName("reducer join");
		
		job.setReducerClass(finalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,CustMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TranMapper.class);

		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);
		//outputPath.getFileSystem(conf).delete(outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
