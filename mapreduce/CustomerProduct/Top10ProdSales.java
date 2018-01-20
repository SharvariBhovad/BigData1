import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

public class Top10ProdSales {
public static class Mapper1 extends Mapper<LongWritable,Text,Text,IntWritable>
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String[] newline=value.toString().split(";");
		String prodid=newline[5];
		int sales=Integer.parseInt(newline[8]);
		context.write(new Text(prodid), new IntWritable(sales));
		
	}
}

public static class Reducer1 extends Reducer<Text,IntWritable,NullWritable,Text>
{
	private TreeMap<IntWritable,Text> mytree=new TreeMap<IntWritable,Text>();
	public void reduce(Text key,Iterable<IntWritable> values, Context context)
	{
		int sum=0;
		String myValue="";
		String mySum="";
		
		for(IntWritable val:values)
		{
			sum+=val.get();
		}
		myValue=key.toString();
		mySum=String.format("%d", sum);
		myValue=myValue+','+mySum;
		mytree.put(new IntWritable(sum), new Text(myValue));
		
		if(mytree.size()>10)
		{
			mytree.remove(mytree.firstKey());
		}	
	}

	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for(Text t: mytree.descendingMap().values())
		{
			context.write(NullWritable.get(),t);
		}
	}
}	
public static void main(String[] args) throws Exception
{
Configuration conf=new Configuration();
Job job=Job.getInstance(conf,"top_10");
job.setJarByClass(Top10ProdSales.class);
job.setMapperClass(Mapper1.class);
job.setReducerClass(Reducer1.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);

job.setNumReduceTasks(1);
job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(Text.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
// hadoop fs -cat /niit_company_output/top10treenew3/p*
//8712045008539,1540503
//4710628131012,675112
//4710114128038,514601
//4711588210441,491292
//20553418     ,470501
//4710628119010,433380
//4909978112950,432596
//8712045000151,428530
//7610053910787,392581
//4719090900065,385626
