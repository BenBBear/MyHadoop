package cn.uc.hadoop.mapreduce.tools.dictreplace.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import cn.uc.hadoop.mapreduce.lib.partition.TextFirstPartitioner;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictDesciptorHelper;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictDescriptor;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceConf;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictTranslate;
import cn.uc.utils.KVPair;

import com.google.common.base.Splitter;

public class TwoStepPickMapper extends Mapper<Text, Text, Text, Text> {
	private static final Log LOG = LogFactory.getLog(Mapper.class);
	private DictReplaceConf conf = null;
//	private List<DictDescriptor> dictDescriptorList = null;
	private DictDesciptorHelper dictDesciptorHelper;
	private String split = null;
	private String secondSortSplit = null;
	private Text writerKey = new Text();
	private Text writervalue = new Text();

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
		split = conf.getSplitString();
		secondSortSplit = conf.get(
				TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR,
				TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR_DEFAULT);
	}
	

	@Override
	// 低效地使用字符串实现 TODO 使用byte继续改写
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String filePath = key.toString();
			DictDescriptor descriptor = dictDesciptorHelper.getDictDescriptor(filePath);
	
			if (descriptor != null) {
				// 字典
				DictTranslate dictTranslate = dictDesciptorHelper.getDictTranslate(filePath);
				if (dictTranslate != null) {
					KVPair<String,String> pair = dictTranslate.translate(value.toString());
					if (pair != null) {
						writerKey.set(descriptor.getFieldNumebr() + split
								+ pair.k + secondSortSplit + "1");
						writervalue.set(pair.v);
						context.write(writerKey, writervalue);
					}
				}
			} else {
				// 日志
				StringBuilder sb = new StringBuilder();
				int index = 1;
				boolean needSplit = false;
				List<String> writeKeyList = new ArrayList<String>();
				for (String field : Splitter.on(split).split(value.toString())) {
					if (dictDesciptorHelper.shouldReplace(index)) {
						if (needSplit) {
							sb.append(split);
						} else {
							needSplit = true;
						}
						sb.append(field);
						writeKeyList.add(index + split + field + secondSortSplit
								+ "2");
					}
					index++;
				}
				writervalue.set(sb.toString());
				for (String k : writeKeyList) {
					writerKey.set(k);
					context.write(writerKey, writervalue);
				}
			}
			
		}
		catch(Exception e){
			Counter counter = context.getCounter("kpi bao", "map paoqi jilu");
			counter.increment(1);
			LOG.error("",e);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}
}