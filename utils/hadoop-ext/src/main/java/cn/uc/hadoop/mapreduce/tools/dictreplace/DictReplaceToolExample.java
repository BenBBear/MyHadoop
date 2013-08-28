package cn.uc.hadoop.mapreduce.tools.dictreplace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.uc.utils.KVPair;
/**
 * DictReplaceTool 类的使用方法
 * @author qiujw
 *
 */
public class DictReplaceToolExample {
	static public class RealDictTranslate extends DictTranslate {

		@Override
		public KVPair<String,String> translate(String line) {
			StringTokenizer st = new StringTokenizer(line, "``");
			int index = 1;
			String k = null, v = null;
			while (st.hasMoreTokens()) {
				if (index == 1) {
					// id=
					k = st.nextToken().substring(3);
				} else if (index == 2) {
					v = st.nextToken().substring(5);
				}

				index++;
			}
			return new KVPair<String,String>(k, v);
		}

	}

	static public class Dict12Translate extends DictTranslate {

		@Override
		public KVPair<String,String> translate(String line) {
			String[] temp = line.split("`");
			String k = null, v = null;
			if (temp.length >= 2) {
				String[] t1 = temp[0].split("=");
				if (t1.length == 2)
					k = t1[1];
				String[] t2 = temp[1].split("=");
				if (t2.length == 2)
					v = t2[1];
			}
			if (k != null && v != null)
				return new KVPair<String,String>(k, v);
			else
				return null;
		}

	}

	static public class Dict1Translate extends DictTranslate {

		@Override
		public KVPair<String,String> translate(String line) {
			String[] temp = line.split("`");
			String k = null, v = null;
			if (temp.length >= 2) {
				String[] t1 = temp[0].split("=");
				if (t1.length == 2)
					k = t1[1];
				String[] t2 = temp[1].split("=");
				if (t2.length == 2)
					v = t2[1];
			}
			if (k != null && v != null)
				return new KVPair<String,String>(k, v);
			else
				return null;
		}

	}

	static public void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();

		// 设置job的名字
		DictReplaceTool tool = new DictReplaceTool(conf, "dict replace",
				DictTaskType.TWOSTEPS);
		// 设置替换规则 ，字典，列数,并添加字典
		List<DictDescriptor> dictDescriptorList = new ArrayList<DictDescriptor>();
		dictDescriptorList.add(new DictDescriptor(
				new Path("/dict/dict/11dict"), 1, Dict1Translate.class));
		dictDescriptorList.add(new DictDescriptor(
				new Path("/dict/dict/112dict"), 2, Dict12Translate.class));
		System.out.println(DictDescriptor.arrayToJson(dictDescriptorList));
		tool.getDictReplaceConf().setDictList(dictDescriptorList);
		// 替换日志中的分隔符 TODO 支持接口读取
		tool.getDictReplaceConf().setSplitString(",");
		// 添加输入的日志
		tool.getDictReplaceConf().addInput(new Path("/dict/in2"));

		Path out = new Path("/dict/out");
		FileSystem fs = FileSystem.get(out.toUri(), conf);
		fs.delete(out, true);

		tool.getDictReplaceConf().setOutput(out);
		tool.waitForCompetion(true);
	}
}
