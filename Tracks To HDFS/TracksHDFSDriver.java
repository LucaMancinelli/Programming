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


public class TracksHDFSDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://localhost:54310");
		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "ADD COMMENT HERE");
		job.setJarByClass(TracksHDFSDriver.class);
		TableMapReduceUtil.initTableMapperJob("FMA_Results", scan, 
											  TracksHDFSMapper.class, 
				                              Text.class, IntWritable.class, 
				                              job);
		job.setReducerClass(TracksHDFSReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path("/user/hduser/tracks/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
