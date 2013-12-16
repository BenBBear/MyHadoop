/**
 * 
 */
package com.uc.ssp.user;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.uc.ssp.common.Const;
import com.uc.ssp.common.Record;
import com.uc.ssp.common.Util;

/**
 * @author yaoy
 * @project ssp2.2_new
 * @package com.uc.ssp.user
 * @date 2012-7-3下午7:40:56
 */
public class NewAndSilenceMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text KEY = new Text(); // KEY文本格式
	Text VALUE = new Text(); // value的文本格式
	private Configuration conf = new Configuration();
	private final Record record = new Record();
	UserConf userConf = new UserConf();
	UserConf preUserConf = new UserConf();

	/**
	 * @param fs
	 * @throws IOException
	 */
	@Override
	public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
		String path = ((FileSplit) context.getInputSplit()).getPath().toString();
		String line = new String(input.getBytes(), 0, input.getLength(), "UTF-8"); // 读取记录
		// 读取总表的记录
		if (path.indexOf("/wholetable/") >= 0) {
			int splitloc = line.indexOf("`", 1);
			if (splitloc < 0) {
				throw new IOException("line is wrong;;" + line);
			}
			KEY.set(line.substring(0, splitloc));
			VALUE.set(line.substring(splitloc + 1) + Const.MAIN_FIELD_SEPARATE + "0");
			context.write(KEY, VALUE);
		} else { // /读取按天汇总的记录
			record.reset();
			record.setRecord(line);
			record.setSplitChar(Const.RECORD_FIELD_SEPARATE);
			// stat的总长度
			int totallength = userConf.statConf.totalFieldNumber;
			// 判断总长度
			if (record.fieldSize() != totallength) {
				throw new IOException("line is wrong;;" + line + ";;record.length==" + record.fieldSize() + ";;totallength==" + totallength);
			}
			StringBuilder feature = new StringBuilder();
			// 位置列表
			List<Integer> totalLocation = userConf.totalLocation;
			if (record.field(totalLocation.get(totalLocation.size() - 1)).equals("0")) {

			} else {
				KEY.set(record.field(totalLocation.get(0)).toString());
				for (int j = 1, n = totalLocation.size() - 1; j < n; j++) {
					int location = totalLocation.get(j);
					record.field(location).appendTo(feature);
					feature.append(Const.SUB_FIELD_SEPARATE);
				}
				if (feature.length() == 0) {
					throw new IOException("newandsilenceUser the total is not the pvindex");
				}
				feature.setLength(feature.length() - 1);
				VALUE.set(feature.toString() + Const.MAIN_FIELD_SEPARATE + "1");
				context.write(KEY, VALUE);
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		// System.out.println(((FileSplit) context.getInputSplit()).getPath());
		try {
			userConf = (UserConf) Util.getObject(Util.decode(conf.get("userConf")));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}
}
