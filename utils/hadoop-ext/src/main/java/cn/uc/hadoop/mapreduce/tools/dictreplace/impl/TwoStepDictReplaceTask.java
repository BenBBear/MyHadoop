package cn.uc.hadoop.mapreduce.tools.dictreplace.impl;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.uc.hadoop.JobUtils;
import cn.uc.hadoop.mapreduce.lib.input.FilePathTextInputFormat;
import cn.uc.hadoop.mapreduce.lib.partition.TextFirstGroupComparator;
import cn.uc.hadoop.mapreduce.lib.partition.TextFirstPartitioner;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictDescriptor;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceConf;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceTask;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceTool;
import cn.uc.hadoop.mapreduce.tools.dictreplace.mapreduce.TwoStepPickMapper;
import cn.uc.hadoop.mapreduce.tools.dictreplace.mapreduce.TwoStepPickReducer;
import cn.uc.hadoop.mapreduce.tools.dictreplace.mapreduce.TwoStepReplaceMapper;
import cn.uc.hadoop.mapreduce.tools.dictreplace.mapreduce.TwoStepReplaceReducer;

public class TwoStepDictReplaceTask implements DictReplaceTask {

	private Job pickJob = null;
	private Job replaceJob = null;
	private Path pickOutput = null;

	public TwoStepDictReplaceTask(DictReplaceConf conf) throws IOException {
		setupPickJob(conf);
		setupReplaceJob(conf);
	}

	private void setupPickJob(DictReplaceConf conf) throws IOException {
		String oldName = conf.get(JobContext.JOB_NAME);
		pickJob = Job.getInstance(conf, oldName + " step-1");
		pickJob.setJarByClass(DictReplaceTool.class);
		pickJob.setOutputKeyClass(Text.class);
		pickJob.setOutputValueClass(Text.class);
		pickJob.setInputFormatClass(FilePathTextInputFormat.class);
		pickJob.setOutputFormatClass(TextOutputFormat.class);
		pickJob.setMapperClass(TwoStepPickMapper.class);
		pickJob.setReducerClass(TwoStepPickReducer.class);
		// pickJob.setCombinerClass(UniqCombiner.class);

		// below conf base on conf
		// TODO
		// 自动使用output加入pick文件前缀作为pickjob的中间输出路径
		pickOutput = getPickOutput(conf);
		FileSystem fs = pickOutput.getFileSystem(conf);
		fs.delete(pickOutput, true);
		FileOutputFormat.setOutputPath(pickJob, pickOutput);
		// 输入文件
		for (Path p : conf.getInputs()) {
			FileInputFormat.addInputPath(pickJob, p);
		}
		// 添加输入字典
		for (DictDescriptor dictDescriptor : conf.getDictList()) {
			FileInputFormat.addInputPath(pickJob, dictDescriptor.getPath());
		}
		// ***
		// 使用二次排序
		// 设置分区函数
		pickJob.setPartitionerClass(TextFirstPartitioner.class);
		// 设置reduce分组判断函数
		pickJob.setGroupingComparatorClass(TextFirstGroupComparator.class);
		JobUtils.setNumberOfReducers(pickJob);
	}

	private Path getPickOutput(DictReplaceConf conf) {
		Path realOutput = new Path(conf.getOutput());
		String defaultPickOutput = realOutput.getParent() + "/pick"
				+ realOutput.getName();
		String pickOutput = conf.get(DictReplaceConf.DICT_REPLACE_PICK_OUTPUT,
				defaultPickOutput);
		return new Path(pickOutput);
	}

	private void setupReplaceJob(DictReplaceConf conf) throws IOException {
		String oldName = conf.get(JobContext.JOB_NAME);
		replaceJob = Job.getInstance(conf, oldName + " step-2");
		replaceJob.setJarByClass(DictReplaceTool.class);
		replaceJob.setMapOutputKeyClass(Text.class);
		replaceJob.setMapOutputValueClass(Text.class);
		replaceJob.setOutputKeyClass(Text.class);
		replaceJob.setOutputValueClass(NullWritable.class);
		replaceJob.setInputFormatClass(FilePathTextInputFormat.class);
		replaceJob.setOutputFormatClass(TextOutputFormat.class);
		replaceJob.setMapperClass(TwoStepReplaceMapper.class);
		replaceJob.setReducerClass(TwoStepReplaceReducer.class);
		// below conf base on conf
		FileOutputFormat.setOutputPath(replaceJob, new Path(conf.getOutput()));
		// 输入文件
		for (Path p : conf.getInputs()) {
			FileInputFormat.addInputPath(replaceJob, p);
		}
		// 添加可能的结果组合
		FileInputFormat.addInputPath(replaceJob, pickOutput);

		// ***
		// 使用二次排序
		// 设置分区函数
		replaceJob.setPartitionerClass(TextFirstPartitioner.class);
		// 设置reduce分组判断函数
		replaceJob.setGroupingComparatorClass(TextFirstGroupComparator.class);

		replaceJob.getConfiguration()
				.set(DictReplaceConf.DICT_REPLACE_PICK_OUTPUT,
						pickOutput.toString());

		replaceJob.getConfiguration().set(
				DictReplaceConf.DICT_REPLACE_FIELD_SEPARATOR,
				conf.getSplitString());
		JobUtils.setNumberOfReducers(replaceJob);
	}

	@Override
	public boolean waitForCompletion(boolean verbose) throws IOException,
			InterruptedException, ClassNotFoundException {
		boolean ok = pickJob.waitForCompletion(verbose);
		if (ok) {
			return replaceJob.waitForCompletion(verbose);
		} else {
			DictReplaceTool.DictReplaceLOG
					.error("pick job is not success while running two step job.");
		}
		return false;
	}
}
