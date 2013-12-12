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
import com.uc.ssp.common.HexBitSet;
import com.uc.ssp.common.Record;
import com.uc.ssp.common.Util;

/**
 * @author yaoy
 * @project ssp2.2_new
 * @package com.uc.ssp.user
 * @date 2012-7-5下午1:29:50
 */
public class GernateActiveTableReducer extends Reducer<Text, Text, Text, Text> {
	public static String Cal120(String data120flag, int statflag) {
		String pristring = data120flag.substring(0, 15);// 先取前15位
		String nextstring = data120flag.substring(15, data120flag.length());// 后取后15位
		long prilong = Long.parseLong(pristring, 16); // 前15位变成10进制
		long nextlong = Long.parseLong(nextstring, 16);// 后15位变成10进制
		long n = nextlong >> 59; // 后15位右移动59位得到最高位
		long priweilong = prilong << 1;// 前15位左移一位
		long prilastlong = priweilong | n;// 后15位第一位移到前15位
		if (prilastlong >= 1152921504606846976l) {
			prilastlong -= 1152921504606846976l;
		}
		long nextweilong = nextlong << 1; // 后15位左移一位
		long nextlastlong = nextweilong | statflag;// 然后或
		if (nextlastlong >= 1152921504606846976l) {
			nextlastlong -= 1152921504606846976l;
		}
		String lasthexstring = "";
		if (prilastlong == 0) {
			lasthexstring = ZERO_PREFIX[14] + convertToHex(nextlastlong);
		} else {
			lasthexstring = convertToHex(prilastlong) + convertToHex(nextlastlong);
		}
		return lasthexstring;
	}

	//
	// /*
	// * 将字符串变成二进制输出，如果不足字符串长度的四倍，则前面补零
	// */
	// public static String convertToBinary(String t) {
	// Long f = Long.parseLong(t, 16);
	// int stringlength = t.length() * 4;
	// String m = Long.toBinaryString(f);
	// int location = stringlength - m.length();
	// for (int i = 0; i < location; i++) {
	// m = "0" + m;
	// }
	// return m;
	// }

	/*
	 * 将整形变成十六进制字符串输出，如果不足15位，则前面补零。因为我们的字符串是30位，每15位分隔的
	 */
	public static String convertToHex(Long t) {
		String f = Long.toHexString(t);
		int location = 15 - f.length();
		if (location > 0) {
			return ZERO_PREFIX[location - 1] + f;
		}
		return f;
	}

	Text KEY = new Text();
	Text VALUE = new Text();
	UserConf userConf = new UserConf();
	UserConf preUserConf = new UserConf();
	private Configuration conf = new Configuration();
	boolean changeFlag = false;
	List<Integer> featureStatTypeId = new ArrayList<Integer>();

	private final static String[] ZERO_PREFIX = { "0", "00", "000", "0000", "00000", "000000", "0000000", "00000000", "000000000", "0000000000",
			"00000000000", "000000000000", "0000000000000", "00000000000000", "000000000000000", "0000000000000000", "00000000000000000",
			"000000000000000000", "0000000000000000000", "00000000000000000000", "000000000000000000000", "0000000000000000000000",
			"00000000000000000000000", "000000000000000000000000", "0000000000000000000000000", "00000000000000000000000000",
			"000000000000000000000000000", "0000000000000000000000000000", "00000000000000000000000000000", "000000000000000000000000000000",
			"0000000000000000000000000000000", "00000000000000000000000000000000" };

	private final Record record = new Record();
	private final Record statRecord = new Record();

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		Iterator<Text> it = value.iterator(); // 遍历同一个key下的不同value值
		StringBuilder totalBuilder = new StringBuilder(); // 记录总表值
		boolean totalFlag = false; // 是否存在于总表的标志
		boolean statFlag = false;// 是否存在于stat表的标志
		while (it.hasNext()) {
			String line = it.next().toString();
			// 区分该条记录来自总表还是stat表，如果第二个字段为0来自总表，如果为1来自stat表
			int firstSplitIdx = line.lastIndexOf(Const.MAIN_FIELD_SEPARATE);// 第一级分割
			if (firstSplitIdx < 0) {
				throw new IOException("line is wrong;;" + line);
			}
			String realValue = line.substring(0, firstSplitIdx);
			// 来自总表。。则需要将旧json表格式变成新的表json格式
			if (line.charAt(line.length() - 1) == '0' && firstSplitIdx == line.length() - 3) { // 来自总表。。则需要将旧json表格式变成新的表json格式
				totalFlag = true;
				record.reset();
				record.setRecord(realValue);
				record.setSplitChar(Const.RECORD_FIELD_SEPARATE);

				if (changeFlag) {
					if (record.fieldSize() != preUserConf.totalFeatureTypeId.size() * 2 + 5) { // 如果解析总表得到的字段小于总表json长度
						throw new IOException("record==" + record.fieldSize() + ";;totalFeatureList==" + preUserConf.totalFeatureTypeId.size()
								+ ";;record==" + record.toString());
					}
					// 遍历新json中的每一个维度，如果在旧表json中存在，则复制过来，如果不存在则记录为" "
					for (int i = 0, n = featureStatTypeId.size(); i < n; i++) {
						int statTypeId = featureStatTypeId.get(i);
						if (preUserConf.totalIndex.containsKey(statTypeId)) {
							int idx = 2 * preUserConf.totalIndex.get(statTypeId);
							record.field(idx).appendTo(totalBuilder);
							totalBuilder.append(Const.SUB_FIELD_SEPARATE);
							record.field(idx + 1).appendTo(totalBuilder);
							totalBuilder.append(Const.SUB_FIELD_SEPARATE);
						} else {
							totalBuilder.append(" ` `");
						}
					}
					// 记录总表的创建日期，最后一次修改日期和120天上线情况
					for (int i = 2 * preUserConf.totalFeatureTypeId.size(); i < 2 * preUserConf.totalFeatureTypeId.size() + 5; i++) {
						record.field(i).appendTo(totalBuilder);
						totalBuilder.append(Const.SUB_FIELD_SEPARATE);
					}
					totalBuilder.setLength(totalBuilder.length() - 1);
				} else {
					if (record.fieldSize() != featureStatTypeId.size() * 2 + 5) { // 如果解析总表得到的字段小于总表json长度
						throw new IOException("record==" + record.fieldSize() + ";;totalFeatureList==" + featureStatTypeId.size() + ";;record=="
								+ record.toString());
					}
					totalBuilder.append(realValue);
				}
			} else if (line.charAt(line.length() - 1) == '1' && firstSplitIdx == line.length() - 3) { // 如果来自新增或者120天之前创建的记录
				statFlag = true;
				statRecord.reset();
				statRecord.setRecord(realValue);
				statRecord.setSplitChar(Const.RECORD_FIELD_SEPARATE);
				if (statRecord.fieldSize() != featureStatTypeId.size() * 2 + 5) { // 如果解析总表得到的字段小于总表json长度
					throw new IOException("statRecord==" + statRecord.fieldSize() + ";;totalFeatureList==" + (featureStatTypeId.size() * 2 + 5)
							+ ";;statRecord==" + statRecord.toString());
				}
			}
		}
		if (totalFlag && statFlag) { // 如果120总表和stat表中都有的记录，则更新总表
			record.reset();
			record.setRecord(totalBuilder.toString());
			record.setSplitChar(Const.RECORD_FIELD_SEPARATE);
			// 重新计算最近120天的上线情况和30天数上线天数
			// 更改PV的值
			// statRecord.field(-3).setValue(value)
			String date120Flag = record.field(-2).toString();
			String date120Hexstring = Cal120(date120Flag, 1);
			statRecord.field(-2).setValue(date120Hexstring);
			int last30Online = HexBitSet.getTrueCount(date120Hexstring, 0, 29);
			statRecord.field(-3).setValue(String.valueOf(last30Online));
			// 设置keyvalue
			KEY.set(key);
			VALUE.set(statRecord.toString());
			context.write(KEY, VALUE);
		} else if (totalFlag && !statFlag) { // 如果仅在120天总表中的记录，如果修改日期是这120天之内，则更新最后一个120位字段
			KEY.set(key);
			record.reset();
			record.setRecord(totalBuilder.toString());
			record.setSplitChar(Const.RECORD_FIELD_SEPARATE);
			if (record.fieldSize() < featureStatTypeId.size() * 2 + 5) { // 如果解析总表得到的字段小于总表json长度
				throw new IOException("record==" + record.fieldSize() + ";;totalFeatureList==" + featureStatTypeId.size() + ";;newtotalinforesult;;"
						+ totalBuilder.toString());
			}
			// 判断总表的最后活跃日期是不是最后120天中的最前一天，如果是则不收回了，否则，修改上线天数和120天上线情况
			String active120day = Util.addDate(userConf.begintime, Const.DATE_FORMAT_B, -120, Const.STAT_PERIOD_DAY);
			Date createdate = null;
			try {
				createdate = Util.str2Date(record.field(-4).toString(), Const.DATE_FORMAT_B);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long createDayDiff = (userConf.begintime.getTime() - createdate.getTime()) / (3600 * 24 * 1000);
			if (createDayDiff >= 120) {
				return;
			} else {
				// 获得原总表中的倒数第二个字段，120天上线情况
				String date120Flag = record.field(-2).toString();
				String date120Hexstring = Cal120(date120Flag, 0);
				record.field(-2).setValue(date120Hexstring);
				int last30Online = HexBitSet.getTrueCount(date120Hexstring, 0, 29);
				record.field(-3).setValue(String.valueOf(last30Online));
				record.field(-1).setValue("0");
				KEY.set(key);
				VALUE.set(record.toString());
				context.write(KEY, VALUE);
			}
		} else if (!totalFlag && statFlag) { // 如果是新增的用户或者沉默用户，则
			KEY.set(key);
			VALUE.set(statRecord.toString());
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
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}
}
