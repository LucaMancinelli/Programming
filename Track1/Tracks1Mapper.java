import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Tracks1Mapper extends 
       TableMapper<Text, IntWritable> {

	private Text genre= new Text();
	private IntWritable track_length = new IntWritable();
FloatWritable();
	
	public void map(ImmutableBytesWritable row, Result columns, Context context)
	       throws IOException, InterruptedException {
		
		String temp_genre = new String(columns.getValue("cf1".getBytes(), "track_genre_top".getBytes()));
		genre.set(temp_genre);

		String temp_track_length = new String (columns.getValue("cf1".getBytes(), "track_duration".getBytes()));
		track_length.set(Integer.parseInt(temp_track_length));
		

		context.write(genre, track_length);
	}

}
