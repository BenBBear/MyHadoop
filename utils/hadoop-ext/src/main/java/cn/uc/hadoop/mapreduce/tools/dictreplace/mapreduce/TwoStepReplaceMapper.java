package cn.uc.hadoop.mapreduce.tools.dictreplace.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.uc.hadoop.mapreduce.lib.partition.TextFirstPartitioner;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictDesciptorHelper;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceConf;

import com.google.common.base.Splitter;

public class TwoStepReplaceMapper extends Mapper<Text, Text, Text, Text> {
	private static final Log LOG = LogFactory.getLog(Mapper.class);
	private DictReplaceConf conf = null;
	private DictDesciptorHelper dictDesciptorHelper;
	private String split = null;
	private String secondSortSplit = null;
	private String pickOutput = null;
	private String textOutputFormatKVsplit = null;
	private Text writerKey = new Text();
	private Text writerValue = new Text();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = new DictReplaceConf(context.getConfiguration());

		try{
			dictDesciptorHelper = new  DictDesciptorHelper(conf);
		}
		catch(Exception e){
			LOG.error("dictDesciptorHelper init catch error",e);
		}
		
		pickOutput = conf.get(DictReplaceConf.DICT_REPLACE_PICK_OUTPUT);
		split = conf.getSplitString();
		secondSortSplit = conf.get(
				TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR,
				TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR_DEFAULT);
		textOutputFormatKVsplit = conf.get(TextOutputFormat.SEPERATOR, "\t");
	}

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		String filePath = key.toString();
		if (filePath.startsWith(pickOutput)) {
			// 可能的结果日志
			// key为 组合+secondSortSplit+"1",value为替换的值
			String[] kvIn = value.toString().split(textOutputFormatKVsplit);
			if (kvIn.length == 2) {
				writerKey.set(kvIn[0] + secondSortSplit + "1");
				writerValue.set(kvIn[1]);
				context.write(writerKey, writerValue);
			}
		} else {

			// 日志,将替换组合作为key
			// key为 组合+split+"2",value为日志本身
			StringBuilder sb = new StringBuilder();
			int index = 1;
			boolean needSplit = false;
			for (String field : Splitter.on(split).split(value.toString())) {
				if (dictDesciptorHelper.shouldReplace(index)) {
					if (needSplit) {
						sb.append(split);
					} else {
						needSplit = true;
					}
					sb.append(field);
				}
				index++;
			}
			writerKey.set(sb.toString() + secondSortSplit + "2");
			context.write(writerKey, value);

		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}
}