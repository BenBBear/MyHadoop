/**
 * 
 */
package com.uc.ssp.commonrpt;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.uc.ssp.common.Const;
import com.uc.ssp.common.Expression;
import com.uc.ssp.common.Record;
import com.uc.ssp.common.Util;
import com.uc.ssp.gernatesql.BaseCreateSql;

/**
 * @author yaoy
 * @project ssp2.2_new
 * @package com.uc.ssp.commonrpt
 * @date 2012-7-6上午11:05:39
 */
public class CommonReportReducer extends Reducer<Text, Text, Text, Text> {
	Text KEY = new Text();
	Text VALUE = new Text();
	HashMap<Integer, HashMap<String, Integer>> commonmap = new HashMap<Integer, HashMap<String, Integer>>();
	CommonConf commonConf = new CommonConf();
	Date createdate = null;
	Date lastdate = null;
	Date nowdate = null;
	Configuration conf = new Configuration();

	HashMap<Integer, CommonRptInfo> commonRptMap;
	HashMap<Integer, BaseCreateSql> Category_Map = new HashMap<Integer, BaseCreateSql>(); // 存放报表类型与报表Sql的类之间的对应关系
	final private Record record = new Record();
	String reportDate = null;

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

		String keyresult = key.toString();
		int catSplitLoc = keyresult.indexOf(Const.MAIN_FIELD_SEPARATE, 1);
		int lenth = 0;
		Iterator<Text> it = value.iterator();
		CalculateMeasure calculater = new CalculateMeasure();
		while (it.hasNext()) {
			String name = it.next().toString();
			record.reset();
			record.setRecord(name);
			record.setSplitChar(Const.RECORD_FIELD_SEPARATE);
			lenth = record.fieldSize();
			for (int j = 0; j < lenth; j++) {
				try {
					calculater.add(j, record.field(j).toString());
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		StringBuilder measureResult = new StringBuilder(); // 存储指标值
		for (int n = 0; n < lenth; n++) {
			measureResult.append(calculater.get(n));
			measureResult.append(Const.RECORD_FIELD_SEPARATE);
		}
		measureResult.setLength(measureResult.length() - 1);
		CommonRptInfo commonRptInfo = commonRptMap.get(Integer.parseInt(keyresult.substring(0, catSplitLoc)));
		BaseCreateSql CreateSql = Category_Map.get(commonRptInfo.rptCategory);
		try {
			String key_info = keyresult.substring(catSplitLoc + 2);
			key_info = Util.escapeString(key_info); // 转义报表维度维度信息
			// HashMap<String, Integer> map =
			// commonmap.get(commonRptInfo.rptCategory);
			// TODO: 指标过滤
			record.reset();
			record.setRecord(measureResult.toString());
			record.setSplitChar(Const.RECORD_FIELD_SEPARATE);
			if (commonRptInfo.filterAfterList.size() != 0) {
				if (!Expression.calcRpn(commonRptInfo.filterAfterList, commonRptInfo.filterAfterMap, record)) {
					return;
				}
			}
			if (commonRptInfo.db_type == Const.DB_TYPE_MYSQL) {
				String resultsql = null;
				resultsql = CreateSql.CreateSql(reportDate, key_info, measureResult.toString(), commonRptInfo);
				if (resultsql == null || resultsql.equals("")) {
					return;
				}
				String[] resultsqlarray = resultsql.split("``");
				for (int i = 0; i < resultsqlarray.length; i++) {
					KEY.set(commonRptInfo.rptId + "insert.sql");
					VALUE.set(resultsqlarray[i]);
					context.write(KEY, VALUE);
				}
			} else if (commonRptInfo.db_type == Const.DB_TYPE_HIVE) {
				ArrayList<String> resultArray = CreateSql.CreateRecord(reportDate, key_info, measureResult.toString(), commonRptInfo);
				if (resultArray == null) {
					return;
				}
				for (int i = 0; i < resultArray.size(); i++) {
					KEY.set(commonRptInfo.rptId + "insert.sql");
					VALUE.set(resultArray.get(i));
					context.write(KEY, VALUE);
				}
			}
		} catch (ParseException e) {
			throw new IOException(e);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		conf = context.getConfiguration();
		try {
			commonConf = (CommonConf) Util.getObject(Util.decode(conf.get("commonConf")));
			nowdate = commonConf.beginTime;
			commonRptMap = commonConf.commonRptMap;
			BaseRptSqlCategory.GernateMap(Category_Map);
			commonmap = CommonTableMap.gernateMap();
			reportDate = Util.date2Str(commonConf.beginTime, Const.DATE_FORMAT_B);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
