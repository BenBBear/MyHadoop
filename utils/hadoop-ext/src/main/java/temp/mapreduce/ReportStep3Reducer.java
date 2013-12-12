package com.uc.ssp.baserpt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.uc.ssp.common.Const;
import com.uc.ssp.common.Expression;
import com.uc.ssp.common.GernateSql;
import com.uc.ssp.common.SequenceParser;
import com.uc.ssp.common.Util;

public class ReportStep3Reducer extends Reducer<Text, Text, Text, Text>{
	private Configuration conf = null;
	private BaseRptConf baseRptConf = null;
	private String tm  = null;
	
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		Text outputKey = new Text();
		Text outputValue = new Text();
		StringBuilder outputKeyStringBuilder = new StringBuilder();
		
		SequenceParser keyParser = new SequenceParser();
		String keyString = new String(key.getBytes(), 0, key.getLength(), Const.DEFAULT_CHARACTER);
		if(1 > keyParser.parser(keyString)){
			throw new IOException(String.format("Can not find report id in record: %s", keyString));
		}
		int rptId = 0;
		try{
			rptId = Integer.parseInt(keyParser.get(0));
		}catch(NumberFormatException e){
			throw new IOException(String.format("Unavailable report id: %s", keyParser.get(0)));
		}
		if(!baseRptConf.rptMap.containsKey(rptId)){
			throw new IOException(String.format("Can not find configure for report id: %d", rptId));
		}
		RptInfo rptInfo = baseRptConf.rptMap.get(rptId);
		
		long lastTime = System.currentTimeMillis();
		long currentTime = 0;
		TopCalculater topCalculater = new TopCalculater(rptInfo.mea.get(rptInfo.topMeaIndex).isTop);	
		Text thisValue = null;
		Iterator<Text> it = value.iterator();
		while (it.hasNext()) {
			thisValue = it.next();
			String valueString = new String(thisValue.getBytes(), 0, thisValue.getLength(), Const.DEFAULT_CHARACTER);
			SequenceParser valueParser = new SequenceParser();
			if((rptInfo.topDimIndex.size()+rptInfo.mea.size()) != valueParser.parser(valueString)){
				throw new IOException(String.format("Error happened when parsing record: %s", valueString));
			}
			TopNode topNode = new TopNode();
			try{
				topNode.measureValue = Double.parseDouble(valueParser.get(rptInfo.topDimIndex.size()+rptInfo.topMeaIndex));
			}catch(NumberFormatException e){
				continue;
				//throw new IOException(String.format("Failed to exchange measure value %s to double", valueParser.get(rptInfo.topDimIndex.size()+rptInfo.topMeaIndex)));
			}
			topNode.sequenceParser = valueParser;
			topCalculater.insert(topNode);
			
			currentTime = System.currentTimeMillis();
			if(currentTime-lastTime > Const.TIMEOUT_MILLIONSECONDS){
				lastTime = currentTime;
				context.progress();
				//context.setStatus(null);
			}
		}
		outputKeyStringBuilder.append(rptId);
		outputKeyStringBuilder.append(Const.DEFAULT_SQL_FILENAME_TAIL);
		outputKey.set(outputKeyStringBuilder.toString());
		
		HashMap<String, Integer> name2index = new HashMap<String, Integer>();
		SequenceParser sequenceParser = new SequenceParser();
		ArrayList<String> meaValueList = new ArrayList<String>();
		
		if (rptInfo.dbType == Const.DB_TYPE_MYSQL) {
			StringBuilder dimFieldName = new StringBuilder();
			StringBuilder dimValue = new StringBuilder();
			StringBuilder meaFieldName = new StringBuilder();
			StringBuilder meaValue = new StringBuilder();
			StringBuilder update = new StringBuilder();
			dimFieldName.append(String.format("`%s`", Const.STAT_TIME));
			dimValue.append(String.format("'%s'", tm));
			for (int i = 0; i < rptInfo.noneTopDimIndex.size(); i++) {
				dimFieldName.append(String.format(",`%s`", rptInfo.dim.get(rptInfo.noneTopDimIndex.get(i)).dbTableFieldName));
				dimValue.append(String.format(",'%s'", Util.escapeString(keyParser.get(i + 1))));
			}
			for (int i = 0; i < rptInfo.topDimIndex.size(); i++) {
				dimFieldName.append(String.format(",`%s`", rptInfo.dim.get(rptInfo.topDimIndex.get(i)).dbTableFieldName));
			}

			int dimValueLength = dimValue.length();

			switch (rptInfo.sqlType) {
			case Const.SQL_TYPE_INSERT:
				for (int i = 0; i < topCalculater.topList.size(); i++) {
					name2index.clear();
					meaValueList.clear();
					for (int j = 0; j < rptInfo.mea.size(); j++) {
						if (baseRptConf.isSamping == 1
								&& (rptInfo.mea.get(j).algothrim.equals(Const.ALGORITHM_CNT) || rptInfo.mea.get(j).algothrim
										.equals(Const.ALGORITHM_UNI))) {
							meaValueList.add(String.format("%d", Math.round(Long.parseLong(topCalculater.topList.get(i).sequenceParser
									.get(rptInfo.topDimIndex.size() + j))
									/ baseRptConf.samplingRate)));
						} else {
							meaValueList.add(topCalculater.topList.get(i).sequenceParser.get(rptInfo.topDimIndex.size() + j));
						}
						name2index.put(rptInfo.mea.get(j).statTypeFieldName, j);
					}
					sequenceParser.set(meaValueList);
					if (!Expression.calcRpn(rptInfo.afterFilterExpr, name2index, sequenceParser)) {
						continue;
					}

					meaFieldName.setLength(0);
					meaValue.setLength(0);
					dimValue.setLength(dimValueLength);

					for (int j = 0; j < rptInfo.topDimIndex.size(); j++) {
						dimValue.append(String.format(",'%s'", Util.escapeString(topCalculater.topList.get(i).sequenceParser.get(j))));
					}
					boolean isFirstMea = true;
					boolean isAllZero = true;
					for (int j = 0; j < rptInfo.mea.size(); j++) {
						String measure = meaValueList.get(j);
						if (measure.equals(Const.DEFAULT_NULL_VALUE)) {
							continue;
						}
						if (isAllZero && Double.parseDouble(measure) != 0.0) {
							isAllZero = false;
						}
						if (!isFirstMea) {
							meaFieldName.append(",");
							meaValue.append(",");
						}
						meaFieldName.append(String.format("`%s`", rptInfo.mea.get(j).dbTableFieldName));
						meaValue.append(measure);
						isFirstMea = false;
					}

					outputValue.set(GernateSql.getStorageSql(rptInfo.tableName, dimFieldName.toString(), dimValue.toString(),
							meaFieldName.toString(), meaValue.toString()));
					if (!isAllZero) {
						context.write(outputKey, outputValue);
					}
				}
				break;
			case Const.SQL_TYPE_INSERT_UPDATE:
				for (int i = 0; i < topCalculater.topList.size(); i++) {
					name2index.clear();
					meaValueList.clear();
					for (int j = 0; j < rptInfo.mea.size(); j++) {
						if (baseRptConf.isSamping == 1
								&& (rptInfo.mea.get(j).algothrim.equals(Const.ALGORITHM_CNT) || rptInfo.mea.get(j).algothrim
										.equals(Const.ALGORITHM_UNI))) {
							meaValueList.add(String.format("%d", Math.round(Long.parseLong(topCalculater.topList.get(i).sequenceParser
									.get(rptInfo.topDimIndex.size() + j))
									/ baseRptConf.samplingRate)));
						} else {
							meaValueList.add(topCalculater.topList.get(i).sequenceParser.get(rptInfo.topDimIndex.size() + j));
						}
						name2index.put(rptInfo.mea.get(j).statTypeFieldName, j);
					}
					sequenceParser.set(meaValueList);
					if (!Expression.calcRpn(rptInfo.afterFilterExpr, name2index, sequenceParser)) {
						continue;
					}

					meaFieldName.setLength(0);
					meaValue.setLength(0);
					dimValue.setLength(dimValueLength);
					update.setLength(0);

					for (int j = 0; j < rptInfo.topDimIndex.size(); j++) {
						dimValue.append(String.format(",'%s'", Util.escapeString(topCalculater.topList.get(i).sequenceParser.get(j))));
					}
					boolean isFirstMea = true;
					boolean isAllZero = true;
					for (int j = 0; j < rptInfo.mea.size(); j++) {
						String measure = meaValueList.get(j);
						if (measure.equals(Const.DEFAULT_NULL_VALUE)) {
							continue;
						}
						if (isAllZero && Double.parseDouble(measure) != 0.0) {
							isAllZero = false;
						}
						if (!isFirstMea) {
							meaFieldName.append(",");
							meaValue.append(",");
							update.append(",");
						}
						meaFieldName.append(String.format("`%s`", rptInfo.mea.get(j).dbTableFieldName));
						meaValue.append(measure);
						update.append(String.format("`%s`=%s", rptInfo.mea.get(j).dbTableFieldName, measure));
						isFirstMea = false;
					}
					if (isAllZero) {
						continue;
					}
					outputValue.set(GernateSql.getStorageSql(rptInfo.tableName, dimFieldName.toString(), dimValue.toString(),
							meaFieldName.toString(), meaValue.toString(), update.toString()));
					context.write(outputKey, outputValue);
				}
				break;
			case Const.SQL_TYPE_UPDATE:
				for (int i = 0; i < topCalculater.topList.size(); i++) {
					name2index.clear();
					meaValueList.clear();
					for (int j = 0; j < rptInfo.mea.size(); j++) {
						if (baseRptConf.isSamping == 1
								&& (rptInfo.mea.get(j).algothrim.equals(Const.ALGORITHM_CNT) || rptInfo.mea.get(j).algothrim
										.equals(Const.ALGORITHM_UNI))) {
							meaValueList.add(String.format("%d", Math.round(Long.parseLong(topCalculater.topList.get(i).sequenceParser
									.get(rptInfo.topDimIndex.size() + j))
									/ baseRptConf.samplingRate)));
						} else {
							meaValueList.add(topCalculater.topList.get(i).sequenceParser.get(rptInfo.topDimIndex.size() + j));
						}
						name2index.put(rptInfo.mea.get(j).statTypeFieldName, j);
					}
					sequenceParser.set(meaValueList);
					if (!Expression.calcRpn(rptInfo.afterFilterExpr, name2index, sequenceParser)) {
						continue;
					}

					StringBuilder whereStr = new StringBuilder();
					whereStr.append(String.format("`%s`='%s'", Const.STAT_TIME, tm));
					for (int j = 0; j < rptInfo.noneTopDimIndex.size(); j++) {
						whereStr.append(String.format(" and `%s`='%s'", rptInfo.dim.get(rptInfo.noneTopDimIndex.get(j)).dbTableFieldName, Util
								.escapeString(keyParser.get(j + 1))));
					}
					for (int j = 0; j < rptInfo.topDimIndex.size(); j++) {
						whereStr.append(String.format(" and '%s'='%s'", rptInfo.dim.get(rptInfo.topDimIndex.get(j)).dbTableFieldName, Util
								.escapeString(topCalculater.topList.get(i).sequenceParser.get(j))));
					}

					StringBuilder meaUpdateStr = new StringBuilder();
					boolean isFirstMea = true;
					boolean isAllZero = true;
					for (int j = 0; j < rptInfo.mea.size(); j++) {
						String measure = meaValueList.get(j);
						if (measure.equals(Const.DEFAULT_NULL_VALUE)) {
							continue;
						}
						if (isAllZero && Double.parseDouble(measure) != 0.0) {
							isAllZero = false;
						}
						if (!isFirstMea) {
							meaUpdateStr.append(",");
						}
						meaUpdateStr.append(String.format("`%s`=%s", rptInfo.mea.get(j).dbTableFieldName, measure));
						isFirstMea = false;
					}
					if (isAllZero) {
						continue;
					}
					outputValue.set(GernateSql.getStorageSql(rptInfo.tableName, meaUpdateStr.toString(), whereStr.toString()));
					context.write(outputKey, outputValue);
				}
				break;
			}
		} else if(rptInfo.dbType == Const.DB_TYPE_HIVE) {
			StringBuilder outputValueStringBuilder = new StringBuilder();
			for (int i = 0; i < topCalculater.topList.size(); i++) {
				outputValueStringBuilder.setLength(0);
				name2index.clear();
				meaValueList.clear();
				for (int j = 0; j < rptInfo.mea.size(); j++) {
					if (baseRptConf.isSamping == 1
							&& (rptInfo.mea.get(j).algothrim.equals(Const.ALGORITHM_CNT) || rptInfo.mea.get(j).algothrim
									.equals(Const.ALGORITHM_UNI))) {
						meaValueList.add(String.format("%d", Math.round(Long.parseLong(topCalculater.topList.get(i).sequenceParser
								.get(rptInfo.topDimIndex.size() + j))
								/ baseRptConf.samplingRate)));
					} else {
						meaValueList.add(topCalculater.topList.get(i).sequenceParser.get(rptInfo.topDimIndex.size() + j));
					}
					name2index.put(rptInfo.mea.get(j).statTypeFieldName, j);
				}
				sequenceParser.set(meaValueList);
				if (!Expression.calcRpn(rptInfo.afterFilterExpr, name2index, sequenceParser)) {
					continue;
				}
				outputValueStringBuilder.append(String.format("%d%s%s%s", rptInfo.sqlType, Const.SUB_FIELD_SEPARATE, tm, Const.SUB_FIELD_SEPARATE));
				for (int j = 0; j < rptInfo.noneTopDimIndex.size(); j++) {
					outputValueStringBuilder.append(keyParser.get(j + 1));
					outputValueStringBuilder.append(Const.SUB_FIELD_SEPARATE);
				}
				for (int j = 0; j < rptInfo.topDimIndex.size(); j++) {
					outputValueStringBuilder.append(String.format("%s%s", topCalculater.topList.get(i).sequenceParser.get(j), Const.SUB_FIELD_SEPARATE));
				}
				for (int j = 0; j < meaValueList.size(); j++) {
					if (j != 0) {
						outputValueStringBuilder.append(Const.SUB_FIELD_SEPARATE);
					}
					outputValueStringBuilder.append(meaValueList.get(j));
				}
				
				outputValue.set(outputValueStringBuilder.toString());
				context.write(outputKey, outputValue);
			}
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException {
		conf = context.getConfiguration();	
		tm = conf.get("tm");
		try {
			baseRptConf = (BaseRptConf) Util.getObject(Util.decode(conf.get("baseRptConf")));
		} catch (ClassNotFoundException e) {
			throw new IOException(e.toString());
		}
	}	
}
