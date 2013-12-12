package com.uc.ssp.merge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.uc.ssp.common.Const;
import com.uc.ssp.common.SequenceParser;
import com.uc.ssp.common.SequencePosition;
import com.uc.ssp.common.Util;

public class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Configuration conf = null;
	private MergeConf mergeConf = null;
	private String tm = null;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String inputValueString = new String(value.getBytes(), 0, value.getLength(), Const.DEFAULT_CHARACTER);

		StringBuilder outputKeyString = new StringBuilder();
		SequencePosition pos = new SequencePosition();
		SequenceParser sequenceParser = new SequenceParser();
		int dimEndIndex = mergeConf.totalFieldNumber - mergeConf.mea.size();
		sequenceParser.parser(inputValueString, dimEndIndex, dimEndIndex, pos);
		int index = inputValueString.indexOf(Const.SEQUENCE_FIELD_SEPARATE);
		if (index == -1 || (pos.endIndex - Const.SEQUENCE_FIELD_SEPARATE.length()) < index || pos.endIndex >= inputValueString.length()) {
			throw new IOException(String.format("Error haddened when parsing record: %s", inputValueString));
		}
		outputKeyString.append(tm);
		outputKeyString.append(inputValueString.substring(index, pos.endIndex - Const.SEQUENCE_FIELD_SEPARATE.length()));
		Text outputKey = new Text(outputKeyString.toString());
		Text outputValue = new Text(inputValueString.substring(pos.endIndex));
		context.write(outputKey, outputValue);
	}

	@Override
	protected void setup(Context context) throws IOException {
		conf = context.getConfiguration();	
		try {
			mergeConf = (MergeConf) Util.getObject(Util.decode(conf.get("mergeConf")));
			tm = conf.get("tm");
		} catch (ClassNotFoundException e) {
			throw new IOException(e.toString());
		}
	}
}
