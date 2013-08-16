package cn.uc.hadoop.mapreduce.tools.dictreplace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DictReplaceConf extends Configuration {
	public static final String DICT_REPLACE_DIST_CACHE_MAX_FILE = "mapreduce.dictreplace.distcache.maxfile";
	public static final long DEFAULT_DICT_REPLACE_DIST_CACHE_MAX_FILE = 1024 * 1024 * 5L;
	public static final String DICT_REPLACE_DESCRIPTOR_LIST = "mapreduce.dictreplace.descriptor.list";
	public static final String DICT_REPLACE_INPUT = "mapreduce.dictreplace.input";
	public static final String DICT_REPLACE_OUTPUT = "mapreduce.dictreplace.output";
	public static final String DICT_REPLACE_PICK_OUTPUT = "mapreduce.dictreplace.pickoutput";
	public static final String DICT_REPLACE_FIELD_SEPARATOR = "mapreduce.dictreplace.field.separator";
	public static final String DEFAULT_DICT_REPLACE_FIELD_SEPARATOR = ",";
	
	public static final String DICT_REPLACE_TASK_TYPE = "mapreduce.dictreplace.task.type";

	public DictReplaceConf(Configuration conf) {
		super(conf);
	}
	
	public void setDictList(DictDescriptor[] dictDescriptorList) {
		set(DICT_REPLACE_DESCRIPTOR_LIST,
				DictDescriptor.arrayToJson(dictDescriptorList));
	}
	
	public void setDictList(List<DictDescriptor> dictDescriptorList) {
		set(DICT_REPLACE_DESCRIPTOR_LIST,
				DictDescriptor.arrayToJson(dictDescriptorList));
	}

	public List<DictDescriptor> getDictList() {
		String json = get(DICT_REPLACE_DESCRIPTOR_LIST);
		if (json == null)
			return new ArrayList<DictDescriptor>();
		return DictDescriptor.jsonToArray(get(DICT_REPLACE_DESCRIPTOR_LIST));
	}

	@SuppressWarnings("deprecation")
	public void addInput(Path inPath) throws IOException {
		FileSystem fs = FileSystem.get(this);
		String newPathStr =  inPath.makeQualified(fs).toString();
		String temp = get(DICT_REPLACE_INPUT);
		if( temp == null) temp = newPathStr;
		else temp += "," + newPathStr;
		set(DICT_REPLACE_INPUT, temp);
	}
	public List<Path> getInputs() {
		List<Path> re = new ArrayList<Path>();
		for(String temp:getTrimmedStrings(DICT_REPLACE_INPUT)){
			re.add(new Path(temp));
		}
		return re;
	}

	@SuppressWarnings("deprecation")
	public void setOutput(Path output) throws IOException {
		//转换为带Scheme的路径
		FileSystem fs = FileSystem.get(this);
		set(DICT_REPLACE_OUTPUT, output.makeQualified(fs).toString());
	}
	public String getOutput() {
		return get(DICT_REPLACE_OUTPUT);
	}
	
	public void setPickOutput(Path output) {
		set(DICT_REPLACE_PICK_OUTPUT, output.toString());
	}
	public String getPickOutput() {
		return get(DICT_REPLACE_PICK_OUTPUT);
	}
	
	public void setSplitString(String split) {
		set(DICT_REPLACE_FIELD_SEPARATOR, split);
	}
	public String getSplitString() {
		return get(DICT_REPLACE_FIELD_SEPARATOR, DEFAULT_DICT_REPLACE_FIELD_SEPARATOR);
	}
	
	public long getDictCacheMaxFile() {
		return getLong(DICT_REPLACE_DIST_CACHE_MAX_FILE,DEFAULT_DICT_REPLACE_DIST_CACHE_MAX_FILE);
	}
	public void setTaskType(DictTaskType type){
		setInt(DICT_REPLACE_TASK_TYPE,type.ordinal());
	}
	public DictTaskType getTaskType(){
		int index =  getInt(DICT_REPLACE_TASK_TYPE,DictTaskType.AUTO.ordinal());
		if( index > DictTaskType.values().length) 
				return DictTaskType.AUTO;
		return DictTaskType.values()[index];
	}
}
