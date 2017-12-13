import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GenreMapper extends 
       TableMapper<Text, IntWritable> {
	
	private Text genre= new Text();
	private IntWritable album_listens = new IntWritable();
	
	public void map(ImmutableBytesWritable row, Result columns, Context context)
	       throws IOException, InterruptedException {
		
		String genre_temp = new String(columns.getValue("cf1".getBytes(), "track_genre_top".getBytes()));
		genre.set(genre_temp);
		String album_listensx_temp = new String (columns.getValue("cf1".getBytes(), "album_listens".getBytes()));
		album_listens.set(Integer.parseInt(album_listens_temp));
				
		
		context.write(genre, album_listens);
	}

}
