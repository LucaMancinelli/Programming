import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;


public class GenreReducer extends
       TableReducer<Text, IntWritable, ImmutableBytesWritable>{
	
	private int total_listens = 0;
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	       throws IOException, InterruptedException {

		int Value =0;
		int sum = 0;
		
		for (IntWritable val : values) {
			Value = val.get();
		sum=sum+Value;

		}
total_listens = sum;
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn("cf1".getBytes(), "total_listens".getBytes(), Bytes.toBytes(Integer.toString(total_listens)));
		put.addColumn("cf1".getBytes(), "genre".getBytes(), Bytes.toBytes(key.toString()));
		
		context.write(null, put); 
	}
}
	


