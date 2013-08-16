package cn.uc.hadoop.mapreduce.tools.dictreplace;

import cn.uc.utils.KVPair;

public abstract class DictTranslate {
	/**
	 * 开发者需要继承DictTranslate，并实现Translate的内容。
	 * line是读入每行的内容,返回。
	 * @param line 读入的文件的每行内容
	 * @return 返回数组的0
	 */
	public abstract KVPair<String,String> translate(String line);
	@SuppressWarnings("unchecked")
	static public DictTranslate getDictTranslate(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		Class<DictTranslate> c = (Class<DictTranslate>) Class.forName(className);
		return c.newInstance();
	}
}
