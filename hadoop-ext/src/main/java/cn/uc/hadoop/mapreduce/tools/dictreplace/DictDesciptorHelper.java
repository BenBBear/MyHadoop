package cn.uc.hadoop.mapreduce.tools.dictreplace;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 用于解析List<DictDesciptor> 对象，生成map或reduce阶段的可用对象
 * @author qiujw
 *
 */
public class DictDesciptorHelper {
	private Set<Integer> pickFieldSet;
	private Map<String, DictDescriptor> dictDescriptorMap;
	private Map<String, DictTranslate> dictTranslateMap;
	private int maxFieldNumber = -1;

	public DictDesciptorHelper(DictReplaceConf conf)
			throws ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		pickFieldSet = Sets.newTreeSet();
		dictDescriptorMap = Maps.newHashMap();
		dictTranslateMap = Maps.newHashMap();
		init(conf);
	}

	private void init(DictReplaceConf conf) throws ClassNotFoundException,
			InstantiationException, IllegalAccessException {
		List<DictDescriptor> dictDescriptorList = conf.getDictList();
		for (DictDescriptor descriptor : dictDescriptorList) {
			if (descriptor.getFieldNumebr() > maxFieldNumber) {
				maxFieldNumber = descriptor.getFieldNumebr();
			}
			pickFieldSet.add(descriptor.getFieldNumebr());

			String filePath = descriptor.getPath().toString();
			dictDescriptorMap.put(filePath, descriptor);

			DictTranslate dictTranslate = null;

			dictTranslate = DictTranslate.getDictTranslate(descriptor
					.getDictTranslate());
			dictTranslateMap.put(filePath, dictTranslate);

		}
	}

	public DictDescriptor getDictDescriptor(String filePath) {
		return dictDescriptorMap.get(filePath);
	}

	public DictTranslate getDictTranslate(String filePath) {
		return dictTranslateMap.get(filePath);
	}

	public boolean shouldReplace(int fieldNumber) {
		return pickFieldSet.contains(fieldNumber);
	}

	public int getMaxFieldNumber() {
		return this.maxFieldNumber;
	}
}
