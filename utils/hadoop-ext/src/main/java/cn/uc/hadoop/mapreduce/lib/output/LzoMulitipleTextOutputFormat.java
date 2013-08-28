package cn.uc.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 * 多输出类。
 * 与 MultipleTextOutputFormat<K,V>类似
 * 使用LzoTextOutputFormat的recordWriter
 * 可在多输出的时候自动建立索引
 * 
 * 使用的时候，继承这个类，并重载generateFileNameForKeyValue函数
 * 
 * 示例
 * public class TryLzoMulitipleTextOutputFormat extends LzoMulitipleTextOutputFormat<Text,IntWritable>{
 * @Override
 * protected String generateFileNameForKeyValue(Text key, IntWritable value, String name) {
 * 	return key.charAt(0) + "-" + name;
 * }
 * }
 * 
 */

public class LzoMulitipleTextOutputFormat<K, V> extends
		MultipleOutputFormat<K, V> {
	@Override
	protected RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job,
			final String name) throws IOException, InterruptedException {
		//修改使用LzoTextOutputFormat
		return new LzoTextOutputFormat<K, V>() {
			// 重要的overwrite
			@Override
			public Path getDefaultWorkFile(TaskAttemptContext context,
					String extension) throws IOException {
				FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
				return new Path(committer.getWorkPath(), name + extension);
			}
		}.getRecordWriter(job);
	}
}