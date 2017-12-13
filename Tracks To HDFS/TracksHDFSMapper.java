import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;

public class TracksHDFSMapper extends 
		TableMapper<Text, Text> {
	
	private Text genre = new Text();
	private Text mean_track_length = new Text();
		
	public void map(ImmutableBytesWritable row, Result columns, Context context)
	       throws IOException, InterruptedException {
		
		String temp_genre = new String (columns.getValue("cf1".getBytes(), "track_genre_top".getBytes()));
		genre.set(temp_genre);
		
		String temp_mean_duration= new String(columns.getValue("cf1".getBytes(), "track_duration".getBytes()));
mean_track_length.set(temp_mean_duration);
		
			
		context.write(genre, mean_track_length);
	}

}

