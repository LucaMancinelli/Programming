
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;;

public class TracksHDFSReducer extends
       Reducer<Text, IntWritable, Text, IntWritable>{
	
	public void reduce(Text key, IntWritable values, Context context)
	       throws IOException, InterruptedException {
		
		
		context.write(key, values);
	}
}
