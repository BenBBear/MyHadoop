package com.uc.ssp.merge;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.uc.ssp.common.Calculater;
import com.uc.ssp.common.Const;
import com.uc.ssp.common.SequenceParser;
import com.uc.ssp.common.Util;

public class MergeReducer extends Reducer<Text, Text, Text, Text> {
	private Configuration conf = null;
	private MergeConf mergeConf = null;

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		SequenceParser sequenceParser = new SequenceParser();
		Calculater calculater = new Calculater();
		Iterator<Text> it = value.iterator();
		while (it.hasNext()) {
			Text itValue = it.next();
			String inputValue = new String(itValue.getBytes(), 0, itValue.getLength(), Const.DEFAULT_CHARACTER);
			if (mergeConf.mea.size() != sequenceParser.parser(inputValue)) {
				throw new IOException("Error happened when parsing measures " + inputValue);
			}
			try {
				for (int i = 0; i < mergeConf.mea.size(); i++) {
					calculater.add(mergeConf.mea.get(i).statTypeFieldId, sequenceParser.get(i), mergeConf.mea.get(i).algorithm);
				}
			} catch (NumberFormatException e) {
				throw new IOException(e.toString());
			} catch (Exception e) {
				throw new IOException(e.toString());
			}
		}

		StringBuilder outputValueStr = new StringBuilder();
		for (int i = 0; i < mergeConf.mea.size(); i++) {
			if (i > 0) {
				outputValueStr.append(Const.SEQUENCE_FIELD_SEPARATE);
			}
			outputValueStr.append(calculater.get(mergeConf.mea.get(i).statTypeFieldId).toString());
		}
		Text outputValue = new Text(outputValueStr.toString());
		context.write(key, outputValue);
	}

	@Override
	protected void setup(Context context) throws IOException {
		conf = context.getConfiguration();
		try {
			mergeConf = (MergeConf) Util.getObject(Util.decode(conf.get("mergeConf")));
		} catch (ClassNotFoundException e) {
			throw new IOException(e.toString());
		}
	}
}
