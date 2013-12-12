package com.uc.ssp.baserpt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.uc.ssp.common.Const;
import com.uc.ssp.common.Expression;
import com.uc.ssp.common.SequenceParser;
import com.uc.ssp.common.Util;

public class ReportStep1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	private Configuration conf = null;
	private BaseRptConf baseRptConf = null;
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputFileHead = ((FileSplit) context.getInputSplit()).getPath().getName().substring(0, baseRptConf.fileHeadLength);
		if(!baseRptConf.fieldIndex.containsKey(inputFileHead)){
			throw new IOException(String.format("Data file %s* with no fieldindex info", inputFileHead));
		}
		FieldIndex fieldIndex = baseRptConf.fieldIndex.get(inputFileHead);
		
		String line = new String(value.getBytes(), 0, value.getLength(), Const.DEFAULT_CHARACTER);
		SequenceParser sequenceParser = new SequenceParser();
		if(fieldIndex.totalFieldCount != sequenceParser.parser(line)){
			throw new IOException(String.format("Error haddened when parsing record: %s", line));
		}
		
		Text outputKey = new Text();
		Text outputValue = new Text();
		StringBuilder outputKeyStringHead = new StringBuilder();
		StringBuilder outputKeyStringTail = new StringBuilder();
		StringBuilder outputValueString = new StringBuilder();
		
		
		Iterator<Entry<Integer, RptInfo>> it = baseRptConf.rptMap.entrySet().iterator();
		while(it.hasNext()){
			Entry<Integer, RptInfo> entry = (Entry<Integer, RptInfo>)it.next();
			RptInfo rptInfo = (RptInfo)entry.getValue();
			outputKeyStringHead.setLength(0);			
			if(!Expression.calcRpn(rptInfo.beforeFilterExpr, fieldIndex.name2index, sequenceParser)){
				continue;
			}
			outputKeyStringHead.append((Integer)entry.getKey());
			outputKeyStringHead.append(Const.SEQUENCE_FIELD_SEPARATE);
			for(int i=0; i<rptInfo.dim.size(); i++){
				outputKeyStringHead.append(sequenceParser.get(fieldIndex.dimSid2index.get(rptInfo.dim.get(i).statTypeFieldId)));
				outputKeyStringHead.append(Const.SEQUENCE_FIELD_SEPARATE);
			}
			Iterator<Entry<Integer, ArrayList<com.uc.ssp.common.Measure>>> meaMapIt = rptInfo.meaMap.entrySet().iterator();
			while(meaMapIt.hasNext()){
				outputKeyStringTail.setLength(0);
				outputValueString.setLength(0);
				Entry<Integer, ArrayList<com.uc.ssp.common.Measure>> meaMapEntry = (Entry<Integer, ArrayList<com.uc.ssp.common.Measure>>)meaMapIt.next();
				int uidTag = (Integer)meaMapEntry.getKey();
				ArrayList<com.uc.ssp.common.Measure> meaList = (ArrayList<com.uc.ssp.common.Measure>)meaMapEntry.getValue();
				outputKeyStringTail.append(uidTag);
				outputKeyStringTail.append(Const.SEQUENCE_FIELD_SEPARATE);
				if(uidTag == 0){
					outputKeyStringTail.append(Const.DEFAULT_NULL_VALUE);
				}else{
					outputKeyStringTail.append(sequenceParser.get(fieldIndex.uid2index.get(uidTag)));
				}
				for(int i=0; i<meaList.size(); i++){
					if(i != 0){
						outputValueString.append(Const.SEQUENCE_FIELD_SEPARATE);
					}
					outputValueString.append(sequenceParser.get(fieldIndex.meaSid2index.get(meaList.get(i).statTypeFieldId)));
				}
				outputKey.set(String.format("%s%s", outputKeyStringHead.toString(), outputKeyStringTail.toString()));
				outputValue.set(outputValueString.toString());
				context.write(outputKey, outputValue);
			}
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException {
		conf = context.getConfiguration();	
		try {
			baseRptConf = (BaseRptConf) Util.getObject(Util.decode(conf.get("baseRptConf")));
		} catch (ClassNotFoundException e) {
			throw new IOException(e.toString());
		}
	}
}
