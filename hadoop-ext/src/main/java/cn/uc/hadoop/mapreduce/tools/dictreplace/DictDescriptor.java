package cn.uc.hadoop.mapreduce.tools.dictreplace;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;

import cn.uc.hadoop.FSUtils;
import cn.uc.utils.Helper;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class DictDescriptor {
	private Path dictPath;
	private String dictTranslate;
	private int fieldNumebr;

	public DictDescriptor(Path dictPath, int fieldNumebr) throws IOException {
		this(dictPath, fieldNumebr, SimpleDictTranslate.class);
	}

	@SuppressWarnings("deprecation")
	public DictDescriptor(Path dictPath, int fieldNumebr,
			Class<? extends DictTranslate> dictTranslateClass)
			throws IOException {
		this.dictPath = dictPath.makeQualified(FSUtils.getDefaultFileSystem());
		this.fieldNumebr = fieldNumebr;
		this.dictTranslate = dictTranslateClass.getName();
	}

	public Path getPath() {
		return dictPath;
	}

	public String getDictTranslate() {
		if (dictTranslate == null) {
			dictTranslate = SimpleDictTranslate.class.getName();
		}
		return dictTranslate;
	}

	public int getFieldNumebr() {
		return fieldNumebr;
	}

	public String toString() {
		return "dictPath:" + dictPath + " fieldNumebr" + fieldNumebr
				+ " dictTranslate:" + dictTranslate;
	}

	static public Gson gson = new Gson();

	static public String arrayToJson(DictDescriptor[] dictDescriptorList) {
		return gson.toJson(dictDescriptorList);
	}

	static public String arrayToJson(List<DictDescriptor> dictDescriptorList) {
		return gson.toJson(dictDescriptorList);
	}

	static public List<DictDescriptor> jsonToArray(String json) {
		Type collectionType = new TypeToken<List<DictDescriptor>>() {
		}.getType();
		return gson.fromJson(json, collectionType);
	}

	static public List<DictDescriptor> readFromFile(String file) {
		FileReader fr = null;
		int inChar;
		String json = "";
		try {

			fr = new FileReader(new File(file));
			while (((inChar = fr.read()) != -1)) {
				json += (char) (inChar);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fr != null) {
				try {
					fr.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return jsonToArray(json);
	}
	//从命令行中获取执行的DictDescriptor
	public static List<DictDescriptor> readFromCommand(String[] dictList) {
		try {
			List<DictDescriptor> dictDescriptorList = Lists.newArrayList();
			// try to parse the dict option
			if (dictList != null && dictList.length > 0) {
				dictDescriptorList = Lists.newArrayList();
				for (String dict : dictList) {
					List<String> list = Helper.split(dict, ",");
					if (list.size() == 2) {
						Path p =new Path(list.get(0));
						int fieldNumber = Integer.valueOf(list.get(1));
						dictDescriptorList.add(new DictDescriptor(p,fieldNumber));
					}
					if (list.size() == 3) {
						//if c can instance successful,then class can found
						Path p =new Path(list.get(0));
						int fieldNumber = Integer.valueOf(list.get(1));
						String className = list.get(2);
						Class<DictTranslate> c = (Class<DictTranslate>) Class.forName(list.get(2));
						DictTranslate temp = DictTranslate.getDictTranslate(className);
						dictDescriptorList.add(new DictDescriptor(p,fieldNumber,c));
					}
					
				}
			}
			return dictDescriptorList;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("the dictDescriptorList should look like 1,/home/hadoop/dict.txt,xxxDictTranslate.class");
		}
		return null;
	}

}
