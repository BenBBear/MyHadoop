package com.uc.ssp.baserpt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.uc.ssp.common.Const;

public class ReportStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = new String(value.getBytes(), 0, value.getLength(), Const.DEFAULT_CHARACTER);
		int index = line.indexOf(Const.MAIN_FIELD_SEPARATE);
		if(index == -1){
			throw new IOException(String.format("Error happened when parsing record %s", line));
		}
		context.write(new Text(line.substring(0, index)), new Text(line.substring(index+Const.MAIN_FIELD_SEPARATE.length())));
	}

}
