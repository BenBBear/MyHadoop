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
import com.uc.ssp.common.Record;
import com.uc.ssp.common.Util;
import com.uc.ssp.totalrptcal.CalTotalBaseClass;
import com.uc.ssp.totalrptcal.CalTotalUser;

/**
 * @author yaoy
 * @project ssp2.2_new
 * @package com.uc.ssp.commonrpt
 * @date 2012-7-9下午5:53:42
 */
public class CalTotalUserMapper extends Mapper<LongWritable, Text, Text, Text> {

	public static StringBuilder getKeyResult(CommonRptInfo commonRptInfo, Record record) {
		StringBuilder dimensionKey = new StringBuilder();
		for (int i = 0; i < commonRptInfo.rptDimensionList.size(); i++) {
			RptDimension rptDimension = commonRptInfo.rptDimensionList.get(i);
			int location = rptDimension.dimensionLoc;
			record.field(location).appendTo(dimensionKey);
			dimensionKey.append(Const.SUB_FIELD_SEPARATE);
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
	private Configuration conf = new Configuration();
	HashMap<Integer, CommonRptInfo> commonRptMap;

	HashMap<Integer, CalTotalBaseClass> category_Map = new HashMap<Integer, CalTotalBaseClass>(); // 存放报表类型与报表计算的类之间的对应关系

	final private Record record = new Record();

	@Override
	public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
		String line = new String(input.getBytes(), 0, input.getLength(), "UTF-8"); // 读取记录
		// String[] fields = SplitStr.indexStr(line, '`');
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
				createdate = Util.strTDate(record.field(totalNumber - 1).toString(), Const.DATE_FORMAT_B);
			} catch (ParseException e) {
				throw new IOException("line is ==" + line + ";;record==" + record.fieldSize() + ";;length==" + totalNumber);
			}

			long CreateDayDiff = (createdate.getTime() - nowdate.getTime()) / (3600 * 24 * 1000);
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
				if (targetObject.rptCategory != CommonDefine.USER_INCTOTAL_CODE) {
					continue;
				}
				if (targetObject.filterBeforeList.size() != 0) {
					if (!Expression.calcRpn(targetObject.filterBeforeList, targetObject.filterBeforeMap, record)) {
						continue;
					}
				}
				CalTotalUser caltotalclass = new CalTotalUser();
				long RptResult = caltotalclass.calcu(CreateDayDiff);
				VALUE.set(LongBitSet.toBytes(RptResult));

				if (targetObject.rptDimensionList.size() == 0) {
					String keyResult = String.format("%s%s%s", rptId, Const.MAIN_FIELD_SEPARATE, Const.DEFAULT_NULL_VALUE);
					KEY.set(keyResult);
					// VALUE.set("" + RptResult);
					context.write(KEY, VALUE);
				} else {
					dimensionKey = CalTotalUserMapper.getKeyResult(targetObject, record);
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
			totalNumber = commonConf.userInfo.totalDimensionTypeId.size() + 2;
			nowdate = commonConf.beginTime;
			commonRptMap = commonConf.commonRptMap;

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}
}
