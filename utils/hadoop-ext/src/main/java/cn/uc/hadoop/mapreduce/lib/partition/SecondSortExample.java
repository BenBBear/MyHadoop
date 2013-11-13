package cn.uc.hadoop.mapreduce.lib.partition;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.GenericOptionsParser;

import cn.uc.hadoop.JobUtils;

/**
 * <pre>
 * 本例子展示了二次排序的使用方法
 * 二次排序满足了以下需求：
 * 在map中按照 A+分隔符+B的格式输出
 * 在reduce中会获得A相同的所有的记录，且记录是按照B排序
 * 
 * <code>
 * Job job = Job.getInstance(conf, "word count");
 * ...
 * //设置分割的标志
 * job.getConfiguration().set(TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR, field);
 * 
 * //设置分区函数
 * job.setPartitionerClass(TextFirstPartitioner.class);
 *
 * //设置reduce分组判断函数
 * job.setGroupingComparatorClass(TextFirstGroupComparator.class);
 * 
 * //如果需要正序，请使用TextSortComparator
 * //如果需要逆序，请使用ReverseTextSortComparator
 * job.setSortComparatorClass(TextSortComparator.class);
 * job.setSortComparatorClass(ReverseTextSortComparator.class);
 * 
 * 
 * </code>
 * </pre>
 * @author qiujw
 *
 */
public class SecondSortExample {

	static public String field = "``";
	static public int maxInt = 1000 ;

	public static class SecondSortMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text writeKey = new Text();
		private Text writeValue = new Text();
		private Random r = new Random();
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
				context.write(new Text(value.toString()), new Text("1"));
		}
	}

	public static class SecondSortReducer extends
			Reducer<Text, Text, Text, Text> {
		private Random r = new Random();
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			long temp = r.nextLong();
			context.write(new Text(Long.toString(temp)), new Text(Long.toString(temp)));
			for (Text val : values) {
				context.write(key, new Text(val+"*"+Long.toString(temp)));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(SecondSortExample.class);
		job.setMapperClass(SecondSortMapper.class);
		job.setReducerClass(SecondSortReducer.class);
		//second sort
		
		//设置分割的标志
		job.getConfiguration().set(TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR, field);
		//设置分区函数
		job.setPartitionerClass(TextFirstPartitioner.class);
		//设置reduce分组判断函数
		job.setGroupingComparatorClass(TextFirstGroupComparator.class);
		job.setSortComparatorClass(ReverseTextSortComparator.class);
		//疑问解答：为什么不需要设置setSortComparator?
		//因为，使用默认的排序方式已经保证有序
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
//		job.setNumReduceTasks(1);
//		JobUtils.setNumberOfReducers(job);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		JobUtils.setNumberOfReducers(job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	


}