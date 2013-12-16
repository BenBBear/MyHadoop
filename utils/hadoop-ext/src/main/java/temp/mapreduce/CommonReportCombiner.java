/**
 * 
 */
package com.uc.ssp.commonrpt;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.uc.ssp.common.Const;
import com.uc.ssp.common.Record;

/**
 * @author yaoy
 * @project ssp2.2_new
 * @package com.uc.ssp.commonrpt
 * @date 2012-7-6上午11:06:34
 */
public class CommonReportCombiner extends Reducer<Text, Text, Text, Text> {
	Text KEY = new Text();
	Text VALUE = new Text();
	private final Record record = new Record();
	private final LongBitSet lbs = new LongBitSet();

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		int length = 0;
		Iterator<Text> it = value.iterator();
		CalculateMeasure calculater = new CalculateMeasure();
		while (it.hasNext()) {
			Text v = it.next();
			byte[] bb = v.getBytes();
			if (bb.length != v.getLength()) {
				bb = Arrays.copyOf(bb, v.getLength());
			}
			String line = v.toString();
			if (!LongBitSet.valid(bb)) {
				record.reset();
				record.setRecord(line);
				record.setSplitChar(Const.RECORD_FIELD_SEPARATE);
				length = record.fieldSize();
				for (int j = 0; j < length; j++) {
					try {
						calculater.add(j, record.field(j).toString());
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} else {// 数字类型输入
				long bitset = LongBitSet.toBitSet(bb);
				lbs.setBitSet(bitset);
				length = lbs.getCapacity();
				for (int j = 0; j < length; j++) {
					try {
						if (!lbs.get(j)) {
							calculater.add(j, "0");
						} else {
							calculater.add(j, "1");
						}
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}
		}
		StringBuilder test = new StringBuilder();
		for (int n = 0; n < length; n++) {
			test.append("" + calculater.get(n));
			test.append(Const.RECORD_FIELD_SEPARATE);
		}
		test.setLength(test.length() - 1);
		KEY.set(key);
		VALUE.set(test.toString());
		context.write(KEY, VALUE);
	}
}
