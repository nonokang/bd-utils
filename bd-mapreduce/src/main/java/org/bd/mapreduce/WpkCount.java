package org.bd.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 统计<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月8日 上午10:37:13 |创建
 */
public class WpkCount {

	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        String[] ioArgs = new String[] { "/user/udf_prog/input/count", "/user/udf_prog/output/count" };
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: WpkCount count <in> <out>");
            System.exit(2);
        }
		
		FileSystem fileSystem = FileSystem.get(conf);

		if (fileSystem.exists(new Path(otherArgs[1]))) {
			fileSystem.delete(new Path(otherArgs[1]), true);
		}

		Job job = Job.getInstance(conf, WpkCount.class.getSimpleName());

		job.setJarByClass(WpkCount.class);//指定执行类

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setNumReduceTasks(1);
		job.setPartitionerClass(HashPartitioner.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);
		
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
			String[] splited = v1.toString().split(",");
			for (String string : splited) {
				context.write(new Text(string), new LongWritable(1L));
			}
		}
	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context) throws IOException, InterruptedException {
			long sum = 0L;
			for (LongWritable v2 : v2s) {
				sum += v2.get();
			}
			context.write(k2, new LongWritable(sum));
		}
	}
}
