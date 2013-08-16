package cn.uc.hadoop.mapreduce.tools.dictreplace.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.uc.hadoop.mapreduce.lib.partition.TextFirstPartitioner;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictDesciptorHelper;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceConf;

import com.google.common.base.Splitter;

public class TwoStepReplaceReducer extends
		Reducer<Text, Text, Text, NullWritable> {
	private static final Log LOG = LogFactory.getLog(Reducer.class);
	private DictReplaceConf conf = null;
	private DictDesciptorHelper dictDesciptorHelper;
	private String split = null;
	private String secondSortSplit = null;
	private Text writerKey = new Text();
	private Text writerValue = new Text();
	private int maxFieldNumber = -1;
	
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
		maxFieldNumber = dictDesciptorHelper.getMaxFieldNumber();
		split = conf.getSplitString();
		secondSortSplit = conf.get(
				TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR,
				TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR_DEFAULT);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try{
			String[] replaceString = new String[maxFieldNumber+1];
			for(Text value:values){
				String[] temp = key.toString().split(secondSortSplit);
				if (temp.length == 2) {
					
					if ("1".equals(temp[1])) {
						String[] intStringPair = value.toString().split(split);
						if(intStringPair.length==2){
							int fieldNumber = Integer.valueOf(intStringPair[0]);
							replaceString[fieldNumber] = intStringPair[1];
							System.out.println(fieldNumber+" "+intStringPair[1]);
						}
					} else if ("2".equals(temp[1])) {
						StringBuilder sb = new StringBuilder();
						int index = 1;
						boolean needSplit = false;
						boolean replaceFinish = true;
						for (String field : Splitter.on(split).split(value.toString())) {
							if (needSplit) {
								sb.append(split);
							} else {
								needSplit = true;
							}
							
							if (dictDesciptorHelper.shouldReplace(index)) {	
								if(replaceString[index] == null){
//									错误输出 ，输出到错误文件
									//TODO 加入失败处理策略
//									writerKey.set("NOMATCH:"+value.toString());
//									context.write(writerKey, NullWritable.get());
									replaceFinish = false;
									break;
								}
								else{
									sb.append(replaceString[index]);
								}
							}
							else{
								sb.append(field);
							}
							
							index++;
						}
						if(replaceFinish){
							writerKey.set(sb.toString());
							context.write(writerKey, NullWritable.get());
						}
					} else {
						// impossible to run here
					}
				}
			}
		}catch(Exception e){
			LOG.error("reducer catch exception",e);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}
}