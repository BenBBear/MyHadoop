/**
 * 
 */
package com.uc.ssp.commonrpt;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.uc.ssp.common.Const;
import com.uc.ssp.common.Expression;
import com.uc.ssp.common.QuarterInfo;
import com.uc.ssp.common.Record;
import com.uc.ssp.common.Util;
import com.uc.ssp.totalrptcal.CalIndependentUser;
import com.uc.ssp.totalrptcal.CalMonthAddUser;
import com.uc.ssp.totalrptcal.CalMonthFlowUser;
import com.uc.ssp.totalrptcal.CalNewUser;
import com.uc.ssp.totalrptcal.CalTotalBaseClass;

/**
 * @author yaoy
 * @project ssp2.2_new
 * @package com.uc.ssp.commonrpt
 * @date 2012-7-6上午11:05:10
 */
public class CalCommonReportMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static StringBuilder getKeyResult(CommonRptInfo commonRptInfo, Record record) {
		StringBuilder dimensionKey = new StringBuilder();
		for (int i = 0; i < commonRptInfo.rptDimensionList.size(); i++) {
			RptDimension rptDimension = commonRptInfo.rptDimensionList.get(i);
			int location = rptDimension.dimensionLoc;
			if (rptDimension.dimensionUse == 0) {
				record.field(location * 2 - 1).appendTo(dimensionKey);
				dimensionKey.append(Const.SUB_FIELD_SEPARATE);
			} else {
				record.field(location * 2).appendTo(dimensionKey);
				dimensionKey.append(Const.SUB_FIELD_SEPARATE);
			}
		}
		return dimensionKey;

	}

	Text KEY = new Text();
	Text VALUE = new Text();
	// LongWritable VALUE = new LongWritable();
	HashMap<Integer, HashMap<String, Integer>> commonmap = new HashMap<Integer, HashMap<String, Integer>>();
	public static Counter ct = null;
	private final static char SPLIT_CHAR = '`';
	int totalNumber = 0;
	CommonConf commonConf = new CommonConf();
	Date createdate = null;
	Date lastdate = null;
	Date nowdate = null;
	private Configuration conf = new Configuration(); /*
													 * 判断 endtime 是否是一个季度的开始
													 */
	QuarterInfo quarter = new QuarterInfo();
	String month_begin_time = "";
	String month_end_time = "";
	HashMap<Integer, CommonRptInfo> commonRptMap;

	HashMap<Integer, CalTotalBaseClass> category_Map = new HashMap<Integer, CalTotalBaseClass>(); // 存放报表类型与报表计算的类之间的对应关系

	final private Record record = new Record();

	@Override
	public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
		String line = new String(input.getBytes(), 0, input.getLength(), "UTF-8"); // 读取记录
		// String[] fields = SplitStr.indexStr(line, '`');

		String last_mod_time = "";
		String add_time = "";
		record.reset();
		record.setRecord(line);
		record.setSplitChar(SPLIT_CHAR);
		StringBuilder dimensionKey = new StringBuilder();
		// int length = 2 * totalFeatureList.size() + totalKeyList.size() + 4;
		// // 总表总的字段
		if (record.fieldSize() != totalNumber) {
			throw new IOException("line is ==" + line + ";;record==" + record.fieldSize() + ";;length==" + totalNumber);
		} else {
			// 如果120位都是0，则不需要计算该记录
			try {
				createdate = Util.strTDate(record.field(totalNumber - 5).toString(), Const.DATE_FORMAT_B);
				lastdate = Util.strTDate(record.field(totalNumber - 4).toString(), Const.DATE_FORMAT_B);
				last_mod_time = record.field(totalNumber - 4).toString();
				add_time = record.field(totalNumber - 5).toString();
			} catch (ParseException e) {
				throw new IOException(e.getMessage() + record.field(totalNumber - 5).toString() + "|" + record.field(totalNumber - 4).toString()
						+ "|" + nowdate + "|" + record.field(totalNumber - 4).toString() + "|" + add_time + "|" + line + "|" + record.fieldSize()
						+ "|" + totalNumber);
			}
			long CreateDayDiff = (createdate.getTime() - nowdate.getTime()) / (3600 * 24 * 1000);
			long LastDayDiff = (lastdate.getTime() - nowdate.getTime()) / (3600 * 24 * 1000);
			String active30 = record.field(totalNumber - 3).toString();
			String acttive = record.field(totalNumber - 2).toString();
			String activepv = record.field(totalNumber - 1).toString();
			/*
			 * 遍历json中配置的所有任务；
			 */
			Iterator<Entry<Integer, CommonRptInfo>> commonRptSet = commonRptMap.entrySet().iterator();
			while (commonRptSet.hasNext()) {
				Entry<Integer, CommonRptInfo> entry = commonRptSet.next();
				int rptId = entry.getKey();
				CommonRptInfo targetObject = entry.getValue();
				if (!targetObject.effectiveFlag) {
					continue;
				}
				if (targetObject.rptCategory == CommonDefine.USER_INCTOTAL_CODE) {
					continue;
				}
				if (targetObject.filterBeforeList.size() != 0) {
					if (!Expression.calcRpn(targetObject.filterBeforeList, targetObject.filterBeforeMap, targetObject.filterBeforeUserMap, record)) {
						continue;
					}
				}
				if (targetObject.rptCategory == CommonDefine.USER_FLOW_CODE) {
					CalIndependentUser calIndependent = new CalIndependentUser();
					String result = calIndependent.calcu(CreateDayDiff, LastDayDiff, active30, acttive, activepv);
					VALUE.set(result);
				} else if (targetObject.rptCategory == CommonDefine.USER_INC_CODE) {
					CalNewUser calIndependent = new CalNewUser();
					String result = calIndependent.calcu(CreateDayDiff, activepv);
					if (result.equals("0")) {
						continue;
					}
					VALUE.set(result);
				} else {
					long RptResult = 0;
					if (targetObject.rptCategory == CommonDefine.USER_MONTHFLOW_CODE) {
						// 自然月、季度 用户流量
						CalMonthFlowUser cal = new CalMonthFlowUser();
						RptResult = cal.calcu(month_begin_time, month_end_time, last_mod_time, quarter);
					} else if (targetObject.rptCategory == CommonDefine.USER_MONTHINC_CODE) {
						// 自然月、季度 用户新增
						CalMonthAddUser cal = new CalMonthAddUser();
						RptResult = cal.calcu(month_begin_time, month_end_time, add_time, quarter);
					} else {
						CalTotalBaseClass caltotalclass = category_Map.get(targetObject.rptCategory);
						RptResult = caltotalclass.calcu(CreateDayDiff, LastDayDiff, active30, acttive);
					}
					if (LongBitSet.value(RptResult) == 0) {
						continue;
					} else {
						VALUE.set(LongBitSet.toBytes(RptResult));
					}
				}

				if (targetObject.rptDimensionList.size() == 0) {
					String keyResult = String.format("%s%s%s", rptId, Const.MAIN_FIELD_SEPARATE, Const.DEFAULT_NULL_VALUE);
					KEY.set(keyResult);
					// VALUE.set("" + RptResult);
					context.write(KEY, VALUE);
				} else {
					dimensionKey = CalCommonReportMapper.getKeyResult(targetObject, record);
					if (dimensionKey.length() == 0) {
						throw new IOException("input==" + line + ";;" + targetObject.rptDimensionList.toString());
					}
					dimensionKey.setLength(dimensionKey.length() - 1);
					String keyResult = String.format("%s%s%s", rptId, Const.MAIN_FIELD_SEPARATE, dimensionKey.toString());
					KEY.set(keyResult);
					context.write(KEY, VALUE);
				}
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		conf = context.getConfiguration();
		try {
			commonConf = (CommonConf) Util.getObject(Util.decode(conf.get("commonConf")));
			totalNumber = 2 * commonConf.userInfo.totalDimensionTypeId.size() + 6;
			nowdate = commonConf.beginTime;
			commonRptMap = commonConf.commonRptMap;
			BaseRptCategory.GernateMap2(category_Map); // 注册常用报表基本算法
			month_begin_time = Util.date2Str(commonConf.beginTime, Const.DATE_FORMAT_B);
			month_end_time = Util.date2Str(commonConf.endTime, Const.DATE_FORMAT_B);
			quarter = Util.getQuarter(Util.date2Str(commonConf.endTime, Const.DATETIME_FORMAT_A), Const.DATETIME_FORMAT_A);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}
}
