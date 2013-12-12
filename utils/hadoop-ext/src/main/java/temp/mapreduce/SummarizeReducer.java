package com.uc.ssp.summarize;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.uc.ssp.common.Calculater;
import com.uc.ssp.common.Const;
import com.uc.ssp.common.SequenceParser;
import com.uc.ssp.common.Util;

public class SummarizeReducer extends Reducer<Text, Text, Text, Text> {

	private Configuration conf = new Configuration();
	private SummarizeConf summarizeConf = new SummarizeConf();

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		SequenceParser sequenceParser = new SequenceParser();
		Calculater calculater = new Calculater();
		Iterator<Text> it = value.iterator();
		while (it.hasNext()) {
			Text itValue = it.next();
			String inputValueString = new String(itValue.getBytes(), 0, itValue.getLength(), Const.DEFAULT_CHARACTER);
			if (summarizeConf.meaFieldCount != sequenceParser.parser(inputValueString)) {
				throw new IOException("Error happened when parsing measures " + inputValueString);
			}
			try {
				for (int i = 0; i < summarizeConf.mea.size(); i++) {
					calculater.add(summarizeConf.mea.get(i).statTypeFieldId, sequenceParser.get(i), summarizeConf.mea.get(i).algorithm);
				}
			} catch (NumberFormatException e) {
				throw new IOException(e.toString());
			} catch (Exception e) {
				throw new IOException(e.toString());
			}
		}

		StringBuilder outputValueStringBuilder = new StringBuilder();
		for (int i = 0; i < summarizeConf.mea.size(); i++) {
			if (i > 0) {
				outputValueStringBuilder.append(Const.SEQUENCE_FIELD_SEPARATE);
			}
			outputValueStringBuilder.append(calculater.get(summarizeConf.mea.get(i).statTypeFieldId).toString());
		}
		Text outputValue = new Text(outputValueStringBuilder.toString());
		context.write(key, outputValue);
	}

	@Override
	protected void setup(Context context) throws IOException {
		conf = context.getConfiguration();
		try {
			summarizeConf = (SummarizeConf) Util.getObject(Util.decode(conf.get("summarizeConf")));
		} catch (ClassNotFoundException e) {
			throw new IOException(e.toString());
		}
	}

}
