import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;


public class Main{

	
	
	public static void main(String[] args){

		Configuration conf = HBaseConfiguration.create();
		try {
			
			HBaseAdmin admin;
			
			admin = new HBaseAdmin(conf);
			
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("raw"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("time"));
			tableDescriptor.addFamily(new HColumnDescriptor("price"));
			if ( admin.isTableAvailable("raw")){
				admin.disableTable("raw");
				admin.deleteTable("raw");
			}
			admin.createTable(tableDescriptor);


			Job job = Job.getInstance();
			job.setJarByClass(Main.class);
			Job job3 = new Job(conf);
			job3.setJarByClass(Job3.class);
			
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(Job1.Map.class);
			TableMapReduceUtil.initTableReducerJob("raw", null, job);
			job.setNumReduceTasks(0);
			
			
			Scan s = new Scan();
			s.setCaching(500);
			s.setCacheBlocks(false);
			TableMapReduceUtil.initTableMapperJob("raw",s,Job3.MyMapper.class, Text.class, Text.class,job3);
			job3.setMapperClass(Job3.MyMapper.class);
			job3.setReducerClass(Job3.MyReducer.class);
			job3.setNumReduceTasks(1);			
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			
			job.waitForCompletion(true);
			job3.waitForCompletion(true);
			
			admin.close();
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
