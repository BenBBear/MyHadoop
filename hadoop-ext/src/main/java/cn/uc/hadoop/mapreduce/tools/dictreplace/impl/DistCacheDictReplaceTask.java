package cn.uc.hadoop.mapreduce.tools.dictreplace.impl;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.uc.hadoop.mapreduce.tools.dictreplace.DictDescriptor;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceConf;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceTask;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceTool;
import cn.uc.hadoop.mapreduce.tools.dictreplace.mapreduce.DistCacheMapper;


public class DistCacheDictReplaceTask implements DictReplaceTask {

	static public final String DIST_REPLACE_TASK_DICTDESCRIPTOR = "dist.replace.task.descriptor";
	private Job job = null;

	public DistCacheDictReplaceTask(DictReplaceConf conf) throws IOException {

		job = Job.getInstance(conf);
		job.setJarByClass(DictReplaceTool.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(DistCacheMapper.class);
		// job.setReducerClass(DistCacheReducer.class);

		// below conf base on conf
		FileOutputFormat.setOutputPath(job, new Path(conf.getOutput()));

		// 输入文件
		for (Path p : conf.getInputs()) {
			FileInputFormat.addInputPath(job, p);
		}
		// 添加分布式缓存
		for (DictDescriptor dictDescriptor : conf.getDictList()) {
			job.addCacheFile(dictDescriptor.getPath().toUri());
		}
		
		job.setNumReduceTasks(0);

	}

	@Override
	public boolean waitForCompletion(boolean verbose) throws IOException,
			InterruptedException, ClassNotFoundException {
		return job.waitForCompletion(verbose);
	}
}
