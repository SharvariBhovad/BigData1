import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ImageDuplicateMapper extends Mapper<Text,BytesWritable,Text,Text>{
	public void map(Text key,BytesWritable values, Context context) throws IOException, InterruptedException
	{
		String md5Str;
		try{
			md5Str=calculatemd5(values.getBytes());
		}catch(NoSuchAlgorithmException e)
		{
			e.printStackTrace();

            context.setStatus("Internal error - can't find the algorithm for calculating the md5");

            return;
		}
		Text md5Text =new Text(md5Str);
		context.write(md5Text, key);
	}
	static String calculatemd5(byte[] imagedata) throws NoSuchAlgorithmException
	{
		MessageDigest md =MessageDigest.getInstance("MD5");
		md.update(imagedata);
		
		byte[] hash= md.digest();
		
		// conveting byte array to hash
		
		String hexstring =new String();
		for(int i=0;i<hash.length;i++)
		{
			hexstring+=Integer.toString((hash[i] & 0xff) +0*100,16).substring(1);
		}
		return hexstring;
	}
}
