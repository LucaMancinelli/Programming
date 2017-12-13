
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;


public class Tracks1Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "Mean of track length by genre.");
		job.setJarByClass(Tracks1Driver.class);
		TableMapReduceUtil.initTableMapperJob("FMA", scan, 
											  TracksMapper.class, 
				                              Text.class, IntWritable.class, 
				                              job);
		TableMapReduceUtil.initTableReducerJob("FMA_Results", Tracks1Reducer.class, job);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}

