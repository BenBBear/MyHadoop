/**
 * 
 */
package com.uc.ssp.user;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.uc.ssp.common.Const;
import com.uc.ssp.common.Record;
import com.uc.ssp.common.Util;

/**
 * @author yaoy
 * @project ssp2.2_new
 * @package com.uc.ssp.user
 * @date 2012-7-5上午11:37:06
 */
public class GernateWholeTableMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text KEY = new Text();
	Text VALUE = new Text();
	Record record = new Record();
	StringBuilder tmpBuilder = new StringBuilder();
	UserConf userConf = new UserConf();
	UserConf preUserConf = new UserConf();
	private Configuration conf = new Configuration();
	boolean changeFlag = false;
	String date = null;
	List<Integer> featureStatTypeId = new ArrayList<Integer>();

	@Override
	public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
		String path = ((FileSplit) context.getInputSplit()).getPath().toString();
		if (path.indexOf("/wholetable/") >= 0) {
			String line = new String(input.getBytes(), 0, input.getLength(), "UTF-8"); // 读取记录
			int splitlocation = line.indexOf(Const.SUB_FIELD_SEPARATE, 1);
			if (splitlocation < 0) {
				throw new IOException("wholetableline is wrong;;" + line);
			}
			record.reset();
			record.setRecord(line.substring(splitlocation + 1));
			record.setSplitChar(Const.RECORD_FIELD_SEPARATE);
			tmpBuilder.setLength(0);
			if (changeFlag) {
				if (record.fieldSize() != preUserConf.totalFeatureTypeId.size() + 1) {
					throw new IOException("prefeatureLength==" + preUserConf.totalFeatureTypeId.size() + ";;wholetablerecord==" + record.toString());
				}
				// 遍历新json中的每一个维度，如果在旧表json中存在，则复制过来，如果不存在则记录为" "
				for (int i = 0, n = featureStatTypeId.size(); i < n; i++) {
					int statTypeId = featureStatTypeId.get(i);
					if (preUserConf.totalIndex.containsKey(statTypeId)) {
						int idx = preUserConf.totalIndex.get(statTypeId);
						tmpBuilder.append(record.field(idx).toString()).append(Const.SUB_FIELD_SEPARATE);
					} else {
						tmpBuilder.append(" ").append(Const.SUB_FIELD_SEPARATE);
					}
				}
				tmpBuilder.append(record.field(-1));
			} else {
				if (record.fieldSize() != featureStatTypeId.size() + 1) {
					throw new IOException("featureLength==" + featureStatTypeId.size() + ";;wholetablerecord==" + record.toString());
				}
				tmpBuilder.append(line.substring(splitlocation + 1));
			}
			KEY.set(line.substring(0, splitlocation));
			VALUE.set(tmpBuilder.toString());
			context.write(KEY, VALUE);
		} else {
			String line = new String(input.getBytes(), 0, input.getLength(), "UTF-8"); // 读取记录
			String statInfo = line.substring(line.indexOf(Const.SUB_FIELD_SEPARATE, 1) + 1);
			record.reset();
			record.setRecord(statInfo);
			record.setSplitChar(Const.RECORD_FIELD_SEPARATE);
			tmpBuilder.setLength(0);
			if (record.fieldSize() != 2 * featureStatTypeId.size() + 6) {
				throw new IOException("totalLength==" + (2 * featureStatTypeId.size() + 6) + ";;statrecord==" + record.toString());
			}
			for (int i = 1, n = record.fieldSize() - 5; i < n; i = i + 2) {
				record.field(i).appendTo(tmpBuilder);
				tmpBuilder.append(Const.SUB_FIELD_SEPARATE);
			}
			tmpBuilder.append(date);
			KEY.set(record.field(0).toString());
			VALUE.set(tmpBuilder.toString());
			context.write(KEY, VALUE);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		conf = context.getConfiguration();
		try {
			userConf = (UserConf) Util.getObject(Util.decode(conf.get("userConf")));
			preUserConf = (UserConf) Util.getObject(Util.decode(conf.get("preUserConf")));
			if (userConf.tagFlag) {
				changeFlag = CheckTwoTotal.TwoJsonChange(userConf, preUserConf);
			}
			featureStatTypeId = userConf.totalFeatureTypeId;
			date = Util.date2Str(userConf.begintime, Const.DATE_FORMAT_B);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
