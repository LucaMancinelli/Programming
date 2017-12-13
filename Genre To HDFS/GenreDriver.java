import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class GenreDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://localhost:54310");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "total listens by genre from HBase to HDFS");
		job.setJarByClass(GenreDriver.class);
		TableMapReduceUtil.initTableMapperJob("FMA_results", scan, 
											  GenreMapper.class, 
				                              Text.class, IntWritable.class, 
				                              job);
		job.setReducerClass(GenreReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("/user/hduser/Genre/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
