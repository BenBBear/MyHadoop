package cn.uc.hadoop.mapreduce.tools.dictreplace.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.uc.hadoop.mapreduce.lib.partition.TextFirstPartitioner;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceConf;

public class TwoStepPickReducer extends Reducer<Text, Text, Text, Text> {
	private DictReplaceConf conf;
	private String split = null;
	private String secondSortSplit = null;
	private Text writeKey = new Text();
	private Text writeValue = new Text();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = new DictReplaceConf(context.getConfiguration());
		split = conf.getSplitString();
		secondSortSplit = conf.get(
				TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR,
				TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR_DEFAULT);

	}

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		try {
			String matchValue = null;
			String tempKey = key.toString();
			int index = tempKey.indexOf(split);
			int fieldNumber = Integer.valueOf(tempKey.substring(0, index));
			for (Text v : value) {
				String[] temp = key.toString().split(secondSortSplit);
				if (temp.length == 2) {
					if ("1".equals(temp[1])) {
						matchValue = v.toString();
					} else if ("2".equals(temp[1])) {
						writeValue.set(fieldNumber + split + matchValue);
						context.write(v, writeValue);
					} else {
						// impossible to run here
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}
}