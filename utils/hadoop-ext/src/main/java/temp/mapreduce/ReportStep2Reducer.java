package com.uc.ssp.baserpt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.uc.ssp.common.Calculater;
import com.uc.ssp.common.Const;
import com.uc.ssp.common.Expression;
import com.uc.ssp.common.GernateSql;
import com.uc.ssp.common.SequenceParser;
import com.uc.ssp.common.Util;

public class ReportStep2Reducer extends Reducer<Text, Text, Text, Text>{

	private Configuration conf = null;
	private BaseRptConf baseRptConf = null;
	private String tm = null;
	
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		
		Text outputKey = new Text();
		Text outputValue = new Text();
		StringBuilder outputKeyStringBuilder = new StringBuilder();
		StringBuilder outputValueStringBuilder = new StringBuilder();
		
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
		SequenceParser valueParser = new SequenceParser();
		Calculater calculater = new Calculater();
		Text thisValue = null;
		Iterator<Text> it = value.iterator();
		while (it.hasNext()) {
			thisValue = it.next();
			String valueString = new String(thisValue.getBytes(), 0, thisValue.getLength(), Const.DEFAULT_CHARACTER);
			if(2 > valueParser.parser(valueString)){
				throw new IOException(String.format("Can not find uid tag and measures in record: %s", valueString));
			}

			int uidTag = 0;
			try{
				uidTag = Integer.parseInt(valueParser.get(0));
			}catch(NumberFormatException e){
				throw new IOException(String.format("Unavailable uid tag: %s", valueParser.get(0)));
			}
			if(!rptInfo.meaMap.containsKey(uidTag)){
				throw new IOException(String.format("Can not find configure for uid tag: %d", uidTag));
			}
			ArrayList<com.uc.ssp.common.Measure> meaList = rptInfo.meaMap.get(uidTag);
			
			for(int i=0; i<meaList.size(); i++){
				try{
					String algorithm = Const.ALGORITHM_CNT;
					if(uidTag == 0){
						algorithm = meaList.get(i).algorithm;
					}
					calculater.add(meaList.get(i).statTypeFieldId, valueParser.get(i+1), algorithm);
				}catch(Exception e){
					throw new IOException(e.toString());
				}
			}
			currentTime = System.currentTimeMillis();
			if(currentTime-lastTime > Const.TIMEOUT_MILLIONSECONDS){
				lastTime = currentTime;	
				context.progress();
				//context.setStatus(null);
			}
		}
		
		if(rptInfo.isTop){
			outputKeyStringBuilder.append(Reporter.TOP_FILENAME_HEAD);
			outputValueStringBuilder.append(rptId);		
			for(int i=0; i<rptInfo.noneTopDimIndex.size(); i++){
				if(rptInfo.dim.get(rptInfo.noneTopDimIndex.get(i)).isTop == 0){
					outputValueStringBuilder.append(Const.SEQUENCE_FIELD_SEPARATE);
					outputValueStringBuilder.append(keyParser.get(rptInfo.noneTopDimIndex.get(i)+1));
				}
			}
			outputValueStringBuilder.append(Const.MAIN_FIELD_SEPARATE);
			for(int i=0; i<rptInfo.topDimIndex.size(); i++){
				if(rptInfo.dim.get(rptInfo.topDimIndex.get(i)).isTop != 0){
					outputValueStringBuilder.append(keyParser.get(rptInfo.topDimIndex.get(i)+1));
					outputValueStringBuilder.append(Const.SEQUENCE_FIELD_SEPARATE);
				}
			}
			for(int i=0; i<rptInfo.mea.size(); i++){
				if(i != 0){
					outputValueStringBuilder.append(Const.SEQUENCE_FIELD_SEPARATE);
				}
				outputValueStringBuilder.append(calculater.get(rptInfo.mea.get(i).statTypeFieldId));
			}
			outputKey.set(outputKeyStringBuilder.toString());
			outputValue.set(outputValueStringBuilder.toString());
		}else{
			HashMap<String, Integer> name2index = new HashMap<String, Integer>();
			SequenceParser sequenceParser = new SequenceParser();
			ArrayList<String> meaValueList = new ArrayList<String>();
			for(int i=0; i<rptInfo.mea.size(); i++){
				if(baseRptConf.isSamping == 1 && (rptInfo.mea.get(i).algothrim.equals(Const.ALGORITHM_CNT) || rptInfo.mea.get(i).algothrim.equals(Const.ALGORITHM_UNI))){
					meaValueList.add(String.format("%d", Math.round((Long)calculater.get(rptInfo.mea.get(i).statTypeFieldId)/baseRptConf.samplingRate)));
				}else{
					meaValueList.add(calculater.get(rptInfo.mea.get(i).statTypeFieldId).toString());
				}			
				name2index.put(rptInfo.mea.get(i).statTypeFieldName, i);
			}
			sequenceParser.set(meaValueList);
			if(!Expression.calcRpn(rptInfo.afterFilterExpr, name2index, sequenceParser)){
				return;
			}
			
			outputKeyStringBuilder.append(rptId);
			outputKeyStringBuilder.append(Const.DEFAULT_SQL_FILENAME_TAIL);
			if(rptInfo.dbType == Const.DB_TYPE_MYSQL){
				//CREATE_SQL
				StringBuilder dimFieldName = new StringBuilder();
				StringBuilder dimValue = new StringBuilder();
				StringBuilder meaFieldName = new StringBuilder();
				StringBuilder meaValue = new StringBuilder();
				dimFieldName.append(String.format("`%s`", Const.STAT_TIME));
				dimValue.append(String.format("'%s'", tm));
				for(int i=0; i<rptInfo.dim.size(); i++){
					dimFieldName.append(String.format(",`%s`", rptInfo.dim.get(i).dbTableFieldName));
					dimValue.append(String.format(",'%s'", Util.escapeString(keyParser.get(i+1))));
				}
				boolean isFirstMea = true;
				boolean isAllZero = true;
				for(int i=0; i<rptInfo.mea.size(); i++){
					if(meaValueList.get(i).equals(Const.DEFAULT_NULL_VALUE)){
						continue;
					}
					if(isAllZero && Double.parseDouble(meaValueList.get(i)) != 0.0){
						isAllZero = false;
					}
					if(!isFirstMea){
						meaFieldName.append(",");
						meaValue.append(",");
					}
					meaFieldName.append(String.format("`%s`", rptInfo.mea.get(i).dbTableFieldName));
					meaValue.append(meaValueList.get(i));
					isFirstMea = false;
				}
				if(isAllZero){
					return;
				}
				outputKey.set(outputKeyStringBuilder.toString());
				switch(rptInfo.sqlType){
				case Const.SQL_TYPE_INSERT:
					outputValue.set(GernateSql.getStorageSql(rptInfo.tableName, dimFieldName.toString(), dimValue.toString(), meaFieldName.toString(), meaValue.toString()));
					break;
				case Const.SQL_TYPE_UPDATE:
				case Const.SQL_TYPE_INSERT_UPDATE:			
					StringBuilder update = new StringBuilder();
					isFirstMea = true;
					for(int i=0; i<rptInfo.mea.size(); i++){
						if(meaValueList.get(i).equals(Const.DEFAULT_NULL_VALUE)){
							continue;
						}
						if(!isFirstMea){
							update.append(",");
						}
						update.append(String.format("`%s`=%s", rptInfo.mea.get(i).dbTableFieldName, meaValueList.get(i)));
						isFirstMea = false;
					}
					if(rptInfo.sqlType == Const.SQL_TYPE_INSERT_UPDATE){
						outputValue.set(GernateSql.getStorageSql(rptInfo.tableName, dimFieldName.toString(), dimValue.toString(), meaFieldName.toString(), meaValue.toString(), update.toString()));
					}else{
						StringBuilder whereStr = new StringBuilder();
						whereStr.append(String.format("`%s`='%s'", Const.STAT_TIME, tm));
						for(int i=0; i<rptInfo.dim.size(); i++){
							whereStr.append(String.format(" and `%s`='%s'", rptInfo.dim.get(i).dbTableFieldName, Util.escapeString(keyParser.get(i+1))));
						}
						outputValue.set(GernateSql.getStorageSql(rptInfo.tableName, update.toString(), whereStr.toString()));
					}
					break;
				}
			}else if(rptInfo.dbType == Const.DB_TYPE_HIVE){
				outputValueStringBuilder.append(String.format("%d%s%s%s", rptInfo.sqlType, Const.SUB_FIELD_SEPARATE, tm, Const.SUB_FIELD_SEPARATE));
				for(int i=0; i<rptInfo.dim.size(); i++){
					outputValueStringBuilder.append(String.format("%s", keyParser.get(i+1)));
					outputValueStringBuilder.append(Const.SUB_FIELD_SEPARATE);
				}
				for (int i = 0; i < rptInfo.mea.size(); i++) {
					if(i!=0){
						outputValueStringBuilder.append(Const.SUB_FIELD_SEPARATE);
					}
					outputValueStringBuilder.append(meaValueList.get(i));
				}
				outputKey.set(outputKeyStringBuilder.toString());
				outputValue.set(outputValueStringBuilder.toString());
			}
		}
		context.write(outputKey, outputValue);
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
