/**
 * 
 */
package com.uc.ssp.user;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.uc.ssp.common.Const;

/**
 * @author yaoy
 * @project ssp2.2_new
 * @package com.uc.ssp.user
 * @date 2012-7-5下午12:04:34
 */
public class GernateActiveTableMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text KEY = new Text(); // KEY文本格式
	Text VALUE = new Text(); // value的文本格式

	@Override
	public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
		String path = ((FileSplit) context.getInputSplit()).getPath().getName();
		String line = new String(input.getBytes(), 0, input.getLength(), "UTF-8"); // 读取记录
		// 读取总表的记录
		if (path.startsWith(Const.DEFAULT_REDUCE_OUTPUT_FILENAME_HEAD) || path.startsWith(Const.DEFAULT_MAP_OUTPUT_FILENAME_HEAD)) {
			int splitLoc = line.indexOf(Const.SUB_FIELD_SEPARATE, 1);
			if (splitLoc < 0) {
				throw new IOException("activetableline is wrong;;" + line);
			}
			KEY.set(line.substring(0, splitLoc));
			VALUE.set(line.substring(splitLoc + 1) + Const.MAIN_FIELD_SEPARATE + "0");
			context.write(KEY, VALUE);
		} else {
			int splitLoc = line.indexOf(Const.SUB_FIELD_SEPARATE, 1);
			if (splitLoc < 0) {
				throw new IOException("statline is wrong;;" + line);
			}
			int twoSplitLoc = line.substring(splitLoc + 1).indexOf(Const.SUB_FIELD_SEPARATE, 1);
			if (twoSplitLoc < 0) {
				throw new IOException("statline is wrong;;" + line);
			}
			KEY.set(line.substring(splitLoc + 1).substring(0, twoSplitLoc));
			VALUE.set(line.substring(splitLoc + 1).substring(twoSplitLoc + 1) + Const.MAIN_FIELD_SEPARATE + "1");
			context.write(KEY, VALUE);
		}
	}
}
