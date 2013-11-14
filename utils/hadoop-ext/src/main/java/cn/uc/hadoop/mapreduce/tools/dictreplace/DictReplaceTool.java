package cn.uc.hadoop.mapreduce.tools.dictreplace;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cn.uc.hadoop.mapreduce.tools.dictreplace.impl.DistCacheDictReplaceTask;
import cn.uc.hadoop.mapreduce.tools.dictreplace.impl.TwoStepDictReplaceTask;
import cn.uc.utils.Helper;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * <pre>
 * 本job用于进行字段替换,适用于以下场景 假定有A字典文件，B日志文件，需要将B中的某一个字段通过字典文件进行映射 例如： dictA的字典文件格式为
 * 10,iphone 11,nokiaN9 12,xiaomi ... dictB的字典文件格式为 100,china 101,american
 * 102,japan ...
 * 
 * fileB的日志文件格式为 10,100,visit,2013-08-12 11,101,visit,2013-08-12
 * 10,102,visit,2013-08-12 ...
 * 
 * 在指定分隔符为',' A字典文件的映射关系为1->2（第一列映射到第二列）,B日志中要映射的字段为第一字段。 替换后如下：
 * 
 * iphone,china,visit,2013-08-12 nokiaN9,american,visit,2013-08-12
 * iphone,moon,visit,2013-08-12 ...
 * 
 * 注释：
 * 
 * 当前做法 1.判断字典文件的总大小,如果小于临界值直接使用distributionCache运行，在mapper阶段做join
 * 2.输入日志文件+字典，得出所有要替换的字段的可能的字段组合(replaceFieldList)和对应的字典结果
 * 3.将可能的(replaceFieldList)作为key,日志文件和字典作为输入,在reduce阶段替换
 * 
 * TODO 日志解析部分使用接口实现,当前仅支持逗号分隔的字符串格式。允许实现自己喜欢的格式。
 * 
 * </pre>
 * @author qiujw
 * @version 1.1
 */
public class DictReplaceTool implements Tool {

	public static Logger DictReplaceLOG = Logger
			.getLogger(DictReplaceTool.class);

	private DictReplaceConf conf = null;

	// 构造函数
	public DictReplaceTool() throws IOException {
		this(new Configuration(), "DictReplaceTool");
	}

	public DictReplaceTool(Configuration conf) throws IOException {
		this(conf, "DictReplaceTool");
	}

	public DictReplaceTool(Configuration conf, String name) throws IOException {
		this(conf, name, DictTaskType.AUTO);
	}

	public DictReplaceTool(Configuration conf, String name, DictTaskType TYPE)
			throws IOException {
		conf.set(JobContext.JOB_NAME, name);
		this.conf = new DictReplaceConf(conf);
		this.conf.setTaskType(TYPE);
	}

	public DictReplaceConf getDictReplaceConf() {
		return conf;
	}

	public boolean waitForCompetion(boolean verbose) throws IOException,
			InterruptedException, ClassNotFoundException {
		DictReplaceTask task = getDictReplaceTask();
		return task.waitForCompletion(verbose);
	}

	private DictReplaceTask getDictReplaceTask() throws IOException {

		DictTaskType type = conf.getTaskType();
		if (type.equals(DictTaskType.AUTO)) {
			long sum = getDictSum();
			if (sum <= conf.getDictCacheMaxFile()) {
				type = DictTaskType.DISCACHE;

			} else {
				type = DictTaskType.TWOSTEPS;
			}
		}
		switch (type) {
		case DISCACHE:
			DictReplaceLOG.info("using DistCacheDictReplaceTask ");
			return new DistCacheDictReplaceTask(conf);
		case TWOSTEPS:
			DictReplaceLOG.info("using TwoStepDictReplaceTask ");
			return new TwoStepDictReplaceTask(conf);
		default: // impossible to tun here;
		}
		return null;
	}

	private long getDictSum() throws IOException {
		long sum = 0;
		for (DictDescriptor dictDescriptor : conf.getDictList()) {
			Path p = dictDescriptor.getPath();
			FileSystem fs = FileSystem.get(p.toUri(), conf);
			if (!fs.exists(p)) {
				throw new IOException("file not exist:" + p);
			}
			FileStatus fileStatus = fs.getFileStatus(p);
			sum += fileStatus.getLen();
		}
		return sum;
	}

	//
	// below function is support command line
	//
	static public void main(String[] args) throws Exception {
		ToolRunner.run(new DictReplaceTool(), args);
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = new DictReplaceConf(conf);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		CommandLineParser parser = new BasicParser();
		CommandLine cl = parser.parse(options, args);
		String[] otherArgs = cl.getArgs();
		if (cl.getArgs().length != 2) {
			System.out.println("in And out not give!");
			return -1;
		}
		String[] inPaths = otherArgs[0].split(",");
		DictReplaceTool tool = new DictReplaceTool(conf);
		for (String in : inPaths) {
			tool.getDictReplaceConf().addInput(new Path(in));
		}
		// OUTPUT
		Path output = new Path(otherArgs[1]);
		tool.getDictReplaceConf().setOutput(output);
		FileSystem fs = output.getFileSystem(tool.getDictReplaceConf());
		if (fs.exists(output)) {
			System.out.println("output is exist. " + output);
			return -1;
		}
		// build dict
		List<DictDescriptor> dictDescriptorList = parserDictDescriptor(cl);
		if (dictDescriptorList == null || dictDescriptorList.size() == 0) {
			System.out.println("parser dictDescriptorList  error.");
			return -1;
		}
		System.out.println(dictDescriptorList);
		tool.getDictReplaceConf().setDictList(dictDescriptorList);

		tool.waitForCompetion(true);
		return 0;
	}

	static private List<DictDescriptor> parserDictDescriptor(CommandLine cl) {
		try {
			String file = cl.getOptionValue("dictFile");
			if (file != null) {
				return DictDescriptor.readFromFile(file);
			} else {
				String[] dictList = cl.getOptionValues("dict");
				return DictDescriptor.readFromCommand(dictList);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@SuppressWarnings({ "static-access", "serial" })
	static Options options = new Options() {
		{
			Option dictFile = OptionBuilder
					.withArgName("jsonFile")
					.hasArg()
					.withDescription(
							"the file write the dict json in it.the json look like"
									+ " [{\"dictPath\":{\"uri\":\"file:///dict/dict/11dict\"},\"dictTranslate\":\"cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceToolExample$Dict1Translate\",\"fieldNumebr\":1},{\"dictPath\":{\"uri\":\"file:///dict/dict/112dict\"},\"dictTranslate\":\"cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceToolExample$Dict12Translate\",\"fieldNumebr\":2}] .")
					.create("dictFile");
			Option dict = OptionBuilder
					.withArgName("file,fieldNumber,className")
					.hasArg()
					.withDescription(
							"the file write the dict json in it.the json look like"
									+ " [{\"dictPath\":{\"uri\":\"file:///dict/dict/11dict\"},\"dictTranslate\":\"cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceToolExample$Dict1Translate\",\"fieldNumebr\":1},{\"dictPath\":{\"uri\":\"file:///dict/dict/112dict\"},\"dictTranslate\":\"cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceToolExample$Dict12Translate\",\"fieldNumebr\":2}] .")
					.create("dict");
			addOption(dict);
			addOption(dictFile);
		}
	};

}
