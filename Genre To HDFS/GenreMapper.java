import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GenreMapper extends 
       TableMapper<Text, IntWritable> {
	
	private Text genre= new Text();
	private IntWritable total_listens = new IntWritable();
	
	public void map(ImmutableBytesWritable row, Result columns, Context context)
	       throws IOException, InterruptedException {
	
		String total_listens_temp = new String (columns.getValue("cf1".getBytes(), "total_listens".getBytes()));
		total_listens.set(Integer.parseInt(total_listens_temp));
		
		String genre_temp = new String(columns.getValue("cf1".getBytes(), "genre".getBytes()));
		genre.set(genre_temp );
	
		context.write(genre, total_listens);
	}

}
