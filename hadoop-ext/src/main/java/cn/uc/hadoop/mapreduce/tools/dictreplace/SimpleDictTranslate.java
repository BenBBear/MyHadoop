package cn.uc.hadoop.mapreduce.tools.dictreplace;

import cn.uc.utils.KVPair;

public class SimpleDictTranslate extends DictTranslate {

	@Override
	public KVPair<String,String> translate(String line) {
		String[] t = line.split(",");
		if (t.length == 2) {
			return new KVPair<String,String>(t[0], t[1]);
		}
		return null;
	}
}
