package cn.uc.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SummarizeMapper extends Mapper<LongWritable, Text, Text, Text> {

//	private Configuration conf = null;
//	private SummarizeConf summarizeConf = null;
//
//	@Override
//	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//		
//		String inputValueString = new String(value.getBytes(), 0, value.getLength(), Const.DEFAULT_CHARACTER);
//
//		StringBuilder mapperKeyStringBuilder = new StringBuilder();
//		SequencePosition pos = new SequencePosition();
//		SequenceParser sequenceParser = new SequenceParser();
//		int dimBeginIndex = summarizeConf.totalFieldCount - summarizeConf.meaFieldCount - summarizeConf.dimFieldCount + 1;
//		int dimEndIndex = summarizeConf.totalFieldCount - summarizeConf.meaFieldCount;
//
//		if (summarizeConf.dimFieldCount == 0) {
//			sequenceParser.parser(inputValueString, dimEndIndex, dimEndIndex, pos);
//			if (pos.endIndex < Const.SEQUENCE_FIELD_SEPARATE.length()) {
//				throw new IOException(String.format("Error happened when parsing record: %s", inputValueString));
//			}
//		} else {
//			if (summarizeConf.dimFieldCount != sequenceParser.parser(inputValueString, dimBeginIndex, dimEndIndex, pos)) {
//				throw new IOException(String.format("Error happened when parsing record: %s", inputValueString));
//			}
//		}
//		if (pos.endIndex >= inputValueString.length()) {
//			throw new IOException(String.format("Error happened when parsing record: %s", inputValueString));
//		}
//
//		for (int i = 0; i < summarizeConf.dimMapper.size(); i++) {
//			mapperKeyStringBuilder.setLength(0);
//			for (int j = 0; j < summarizeConf.dimMapper.get(i).keyId.size(); j++) {
//				int id = summarizeConf.dimMapper.get(i).keyId.get(j);
//				if (!summarizeConf.dimFieldIndex.containsKey(id)) {
//					throw new IOException(String.format("Error happened when mapping record ", inputValueString));
//				}
//				int index = summarizeConf.dimFieldIndex.get(id);
//				index -= dimBeginIndex;
//
//				mapperKeyStringBuilder.append(sequenceParser.get(index));
//				mapperKeyStringBuilder.append(Const.SEQUENCE_FIELD_SEPARATE);
//			}
//
//			if (!summarizeConf.dimFieldIndex.containsKey(summarizeConf.dimMapper.get(i).valueId)) {
//				throw new IOException(String.format("Error happened when mapping record: %s", inputValueString));
//			}
//			int index = summarizeConf.dimFieldIndex.get(summarizeConf.dimMapper.get(i).valueId);
//			index -= dimBeginIndex;
//
//			if (summarizeConf.dimMapper.get(i).mapper.containsKey(mapperKeyStringBuilder.toString())) {
//				sequenceParser.set(index, summarizeConf.dimMapper.get(i).mapper.get(mapperKeyStringBuilder.toString()));
//			} else {
//				sequenceParser.set(index, summarizeConf.dimMapper.get(i).defaultValue);
//			}
//
//		}
//
//		StringBuilder outputKeyString = new StringBuilder();
//		if (summarizeConf.dimFieldCount == 0) {
//			outputKeyString.append(inputValueString.substring(0, pos.endIndex - Const.SEQUENCE_FIELD_SEPARATE.length()));
//		} else {
//			outputKeyString.append(inputValueString.substring(0, pos.beginIndex));
//		}
//		for (int i = 0; i < summarizeConf.dimFieldCount; i++) {
//			outputKeyString.append(Const.SEQUENCE_FIELD_SEPARATE);
//			outputKeyString.append(sequenceParser.get(i));
//		}
//
//		Text outputKey = new Text(outputKeyString.toString());
//		Text outputValue = new Text(inputValueString.substring(pos.endIndex));
//
//		context.write(outputKey, outputValue);
//	}
//
//	@Override
//	protected void setup(Context context) throws IOException {
//		conf = context.getConfiguration();
//		try {
//			summarizeConf = (SummarizeConf) Util.getObject(Util.decode(conf.get("summarizeConf")));
//		} catch (ClassNotFoundException e) {
//			throw new IOException(e.toString());
//		}
//		loadMapper();
//	}
//
//	private void loadMapper() throws IOException {
//
//		FileSystem fs = FileSystem.get(conf);
//		String strline = null;
//		KVParser kvParser = new KVParser();
//		StringBuilder keyStringBuilder = new StringBuilder();
//		for (int i = 0; i < summarizeConf.dimMapper.size(); i++) {
//			FSDataInputStream input = fs.open(new Path(summarizeConf.dimMapper.get(i).mapperFilePath));
//			BufferedReader read = new BufferedReader(new InputStreamReader(input));
//
//			while ((strline = read.readLine()) != null) {
//				if (!kvParser.parser(strline)) {
//					input.close();
//					throw new IOException("Error happened when parsing mapper file " + summarizeConf.dimMapper.get(i).mapperFilePath);
//				}
//				keyStringBuilder.setLength(0);
//				for (int j = 0; j < summarizeConf.dimMapper.get(i).keyName.size(); j++) {
//					if(!kvParser.containsKey(summarizeConf.dimMapper.get(i).keyName.get(j))){
//						throw new IOException(String.format("Key %s can not found on mapper file, '%s'", summarizeConf.dimMapper.get(i).keyName.get(j), strline));
//					}
//					keyStringBuilder.append(kvParser.get(summarizeConf.dimMapper.get(i).keyName.get(j)));
//					keyStringBuilder.append(Const.SEQUENCE_FIELD_SEPARATE);
//				}
//				if(!kvParser.containsKey(summarizeConf.dimMapper.get(i).valueName)){
//					throw new IOException(String.format("Key %s can not found on mapper file, '%s'", summarizeConf.dimMapper.get(i).valueName, strline));
//				}
//				summarizeConf.dimMapper.get(i).mapper.put(keyStringBuilder.toString(), kvParser.get(summarizeConf.dimMapper.get(i).valueName));
//			}
//			input.close();
//		}
//	}
}
