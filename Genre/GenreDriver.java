
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;


public class GenreDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "Total album listens by Genre");
		job.setJarByClass(GenreDriver.class);
		TableMapReduceUtil.initTableMapperJob("FMA", scan, 
											  GenreMapper.class, 
				                              Text.class, IntWritable.class, 
				                              job);
		TableMapReduceUtil.initTableReducerJob("FMA_Results", GenreReducer.class, job);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}

