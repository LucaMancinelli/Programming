import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;


public class Tracks1Reducer extends
       TableReducer<Text, IntWritable, ImmutableBytesWritable>{

	
	private int num = 0;
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	       throws IOException, InterruptedException {

		int total = 0;
		int mean = 0;
		
		for (IntWritable val : values) {
			total += val.get();
			num = num+1;
		
		}
		
		mean = (total/num);
		num=0;
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn("cf1".getBytes(), "genre".getBytes(), Bytes.toBytes(key.toString()));
		put.addColumn("cf1".getBytes(), "average_length".getBytes(), Bytes.toBytes(Integer.toString(mean)));

		context.write(null, put);
		
		
	}
	

}
