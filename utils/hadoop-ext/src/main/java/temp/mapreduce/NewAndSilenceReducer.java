/**
 * 
 */
package com.uc.ssp.user;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.uc.ssp.common.Const;
import com.uc.ssp.common.Record;
import com.uc.ssp.common.Util;

/**
 * @author yaoy
 * @project ssp2.2_new
 * @package com.uc.ssp.user
 * @date 2012-7-4上午10:57:48
 */
public class NewAndSilenceReducer extends Reducer<Text, Text, Text, Text> {
	Text KEY = new Text();
	Text VALUE = new Text();
	UserConf userConf = new UserConf();
	UserConf preUserConf = new UserConf();
	private Configuration conf = new Configuration();
	boolean changeFlag = false;
	Date nowdate;
	String date = null;
	List<Integer> featureStatTypeId = new ArrayList<Integer>();
	private final String[] ZERO_PREFIX = { "0", "00", "000", "0000", "00000", "000000", "0000000", "00000000", "000000000", "0000000000",
			"00000000000", "000000000000", "0000000000000", "00000000000000", "000000000000000", "0000000000000000", "00000000000000000",
			"000000000000000000", "0000000000000000000", "00000000000000000000", "000000000000000000000", "0000000000000000000000",
			"00000000000000000000000", "000000000000000000000000", "0000000000000000000000000", "00000000000000000000000000",
			"000000000000000000000000000", "0000000000000000000000000000", "00000000000000000000000000000", "000000000000000000000000000000",
			"0000000000000000000000000000000", "00000000000000000000000000000000" };

	public int get_null_count(Record record) {
		int null_count = 0;
		for (int i = 0; i < record.fieldSize() - 1; ++i) {
			if (record.field(i).toString().equals(" ")) {
				null_count++;
			}
		}
		return null_count;
	}

	public void logicFillCompar(Record curRec, Record statRec) {
		if (statRec.record.isEmpty()) {
			statRec.reset();
			statRec.setRecord(curRec.toString());
			return;
		}
		for (int i = 0; i < curRec.fieldSize(); ++i) {
			if (!curRec.field(i).toString().equals(" ")) {
				statRec.field(i).setValue(curRec.field(i).toString());
			}
		}
	}

	public void logicMostCompare(Record curRec, Record statRec) {
		if (statRec.record.isEmpty()) {
			statRec.setRecord(curRec.toString());
			return;
		}
		if (get_null_count(statRec) > get_null_count(curRec)) {
			statRec.reset();
			statRec.setRecord(curRec.toString());
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		Iterator<Text> it = value.iterator(); // 遍历同一个key下的不同value值
		boolean totalflag = false; // 是否存在于总表的标志
		boolean accflag = false;// 是否存在于stat表的标志

		long statPv = 0;
		Record totalRecord = new Record();
		Record statRecord = new Record();
		Record curRecord = new Record();
		totalRecord.setSplitChar(Const.RECORD_FIELD_SEPARATE);
		// statRecord.setSplitChar(Const.RECORD_FIELD_SEPARATE);

		while (it.hasNext()) {
			// 区分该条记录来自总表还是stat表，如果第二个字段为0来自总表，如果为1来自stat表
			String line = it.next().toString();
			int firstSplitIdx = line.lastIndexOf(Const.MAIN_FIELD_SEPARATE);// 第一级分割
			if (firstSplitIdx < 0) {
				throw new IOException("line is wrong;;" + line);
			}
			String realValue = line.substring(0, firstSplitIdx);
			// 来自总表。。则需要将旧json表格式变成新的表json格式
			if (line.charAt(line.length() - 1) == '0' && firstSplitIdx == line.length() - 3) { // 来自总表。。则需要将旧json表格式变成新的表json格式
				totalflag = true;
				curRecord.reset();
				curRecord.setRecord(realValue);
				curRecord.setSplitChar(Const.RECORD_FIELD_SEPARATE);
				StringBuilder builder = new StringBuilder();

				// 如果总表json没有变化，则直接赋值，如果json发生变化，则按照新的json格式生成总表
				if (changeFlag) {
					int featurelength = preUserConf.totalFeatureTypeId.size(); // 旧表的长度
					if (curRecord.fieldSize() != featurelength + 1) {
						// if (changeFlag) {
						throw new IOException("featureLength==" + featurelength + ";;record==" + curRecord.toString());
						// }
					}
					// 遍历新json中的每一个维度，如果在旧表json中存在，则复制过来，如果不存在则记录为" "
					for (int i = 0, n = featureStatTypeId.size(); i < n; i++) {
						int statTypeId = featureStatTypeId.get(i);
						if (preUserConf.totalIndex.containsKey(statTypeId)) {
							int idx = preUserConf.totalIndex.get(statTypeId);
							builder.append((curRecord.field(idx).toString())).append(Const.SUB_FIELD_SEPARATE);
						} else {
							builder.append(" `");
						}
					}
					builder.append(curRecord.field(-1).toString());
					totalRecord.setRecord(builder.toString());
				} else {
					int featurelength = featureStatTypeId.size(); // 表的长度
					if (curRecord.fieldSize() != featurelength + 1) {
						// if (changeFlag) {
						throw new IOException("featureLength==" + featurelength + ";;record==" + curRecord.toString());
						// }
					}
					totalRecord.setRecord(realValue);
				}
				// 得到新json格式的记录
			} else if (line.charAt(line.length() - 1) == '1' && firstSplitIdx == line.length() - 3) { // 如果来自stat表中的记录
				accflag = true;
				curRecord.reset();
				curRecord.setRecord(realValue);
				curRecord.setSplitChar(Const.RECORD_FIELD_SEPARATE);// 记录sat中的信息
				if (curRecord.fieldSize() != featureStatTypeId.size() + 1) {
					throw new IOException("featureLength==" + (featureStatTypeId.size() + 1) + ";;record==" + curRecord.toString());
				}
				// logicFillCompar(curRecord, statRecord);
				logicMostCompare(curRecord, statRecord);
				if (!curRecord.field(featureStatTypeId.size()).toString().equals(Const.DEFAULT_NULL_VALUE)) {
					statPv += (long) Double.parseDouble(curRecord.field(featureStatTypeId.size()).toString());
				}
			} else {
				throw new IOException("record is wrong.;;record==" + line);
			}
		}
		if (userConf.userPvMax != 0) {
			if (statPv > userConf.userPvMax) {
				statPv = userConf.userPvMax;
			}
		}
		if (totalflag && accflag) { // 如果总表和stat表中则记录那些活跃日期在120天之前的那些沉默用户
			// 如果创建日期在120天之前的用户，则记录下来
			Date createdate = null;
			try {
				createdate = Util.strTDate(totalRecord.field(featureStatTypeId.size()).toString(), Const.DATE_FORMAT_B);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long createDayDiff = (nowdate.getTime() - createdate.getTime()) / (3600 * 24 * 1000);
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < totalRecord.fieldSize() - 1; i++) {
				// 用最全的计算
				builder.append(totalRecord.field(i).toString()).append(Const.SUB_FIELD_SEPARATE);
				builder.append(statRecord.field(i).toString()).append(Const.SUB_FIELD_SEPARATE);
				// 用替换非空的方法计算
				// if
				// (!statRecord.field(i).toString().equals(Const.DEFAULT_NULL_VALUE))
				// {
				// builder.append(statRecord.field(i).toString()).append(Const.SUB_FIELD_SEPARATE);
				// } else {
				// builder.append(totalRecord.field(i).toString()).append(Const.SUB_FIELD_SEPARATE);
				// }
			}
			builder.append(totalRecord.field(totalRecord.fieldSize() - 1));
			if (builder.length() == 0) {
				throw new IOException("totalRecord=" + totalRecord.toString() + "||statRecordsize=" + statRecord.toString());
			}
			String valueresult = String.format("%s`%s`%s`1`%s1`%s", key.toString(), builder.toString(), date, ZERO_PREFIX[28], statPv);
			if (createDayDiff > 120) {
				KEY.set(Const.SILENCE_TYPE_FILE);
				VALUE.set(valueresult);
				context.write(KEY, VALUE);
			} else {
				KEY.set(Const.CONSTAIN_TYPE_FILE);
				VALUE.set(valueresult);
				context.write(KEY, VALUE);
			}
		} else if (!totalflag && accflag) {// 如果仅在stat表中有，则为新增用户
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < statRecord.fieldSize() - 1; i++) {
				builder.append(statRecord.field(i).toString()).append(Const.SUB_FIELD_SEPARATE);
				builder.append(statRecord.field(i).toString()).append(Const.SUB_FIELD_SEPARATE);
			}
			builder.append(date);
			String valueresult = String.format("%s`%s`%s`1`%s1`%s", key.toString(), builder.toString(), date, ZERO_PREFIX[28], statPv);
			// 创建日期和修改日期都是新增的日期,以及最近30天上线天数为1
			KEY.set(Const.NEWUSER_TYPE_FILE);
			VALUE.set(valueresult);
			context.write(KEY, VALUE);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		try {
			userConf = (UserConf) Util.getObject(Util.decode(conf.get("userConf")));
			preUserConf = (UserConf) Util.getObject(Util.decode(conf.get("preUserConf")));
			if (userConf.tagFlag) {
				changeFlag = CheckTwoTotal.TwoJsonChange(userConf, preUserConf);
			}
			featureStatTypeId = userConf.totalFeatureTypeId;
			try {
				nowdate = Util.str2Date(Util.date2Str(userConf.begintime, Const.DATE_FORMAT_B), Const.DATE_FORMAT_B);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			date = Util.date2Str(userConf.begintime, Const.DATE_FORMAT_B);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}
}
