package cn.uc.hadoop.mapreduce.tools.dictreplace.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.uc.hadoop.mapreduce.tools.dictreplace.DictDescriptor;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictReplaceConf;
import cn.uc.hadoop.mapreduce.tools.dictreplace.DictTranslate;
import cn.uc.utils.DoubleMap;
import cn.uc.utils.KVPair;

import com.google.common.base.Splitter;

public class DistCacheMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final Log LOG = LogFactory.getLog(Mapper.class);
	// private boolean[] replaceMark = new boolean[16];
	private String split = null;
	private Text writeKey = new Text();
	private Text writeValue = new Text();
	private DictReplaceConf conf = null;
	// TODO 第一层的map换用array实现，减少一层hash
	private DoubleMap<Integer, String, String> cache;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = new DictReplaceConf(context.getConfiguration());
		localCacheMap(context);
		split = conf.getSplitString();
	}

	@Override
	// 低效实现，先简单使用字符串切割，直接使用cache中寻找然后替换
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 将value中的字段进行分割
		int index = 1;
		StringBuilder sb = new StringBuilder();
		for (String in : Splitter.on(split).split(value.toString())) {
			if (index != 1) {
				sb.append(split);
			}
			if (cache.containsKey(index)) {
				String temp = cache.get(index, in);
				if (temp != null) {
					sb.append(temp);
				} else
					sb.append(in);
			} else {
				sb.append(in);
			}
			index++;
		}
		// byte[] vByte = value.getBytes();
		// int pos = UTF8ByteArrayUtils.findBytes(vByte, 0, vByte.length,
		// split.getBytes());
		writeKey.set(sb.toString());
		context.write(writeKey, writeValue);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}

	@SuppressWarnings("deprecation")
	private void localCacheMap(Context context) throws IOException {
		cache = new DoubleMap<Integer, String, String>();
		context.getConfiguration();
		Map<String, Path> map = buildLocalCacheFilesMap(context
				.getLocalCacheFiles());
		for (DictDescriptor dictDescriptor : conf.getDictList()) {
			String fileName = dictDescriptor.getPath().getName();
			Path localPath = map.get(fileName);
			if (localPath == null) {
				LOG.warn("local cache file can't find by name:" + fileName);
				continue;
			}
			// 根据 DictDescriptor 获取对应的DictTranslate
			DictTranslate dictTranslate = null;
			try {
				dictTranslate = DictTranslate.getDictTranslate(dictDescriptor
						.getDictTranslate());
			} catch (Exception e) {
				LOG.warn("DictTranslate's class "
						+ dictDescriptor.getDictTranslate()
						+ " can't instance right.");
			}
			if (dictTranslate == null)
				continue;
			// 使用DictTranslate进行逐行翻译
			String line = null;
			BufferedReader br = null;
			try {
				FileSystem fs = FileSystem.get(localPath.toUri(),
						context.getConfiguration());
				br = new BufferedReader(new InputStreamReader(
						fs.open(localPath)));
				// boolean replace = false;
				while ((line = br.readLine()) != null) {
					KVPair<String,String> pair = dictTranslate.translate(line);
					// 解析每一行,得到一堆KY对,在cache中记录，第N行需要将key替换为value
					// replace = true;
					if (pair != null) {
						cache.put(dictDescriptor.getFieldNumebr(), pair.k,
								pair.v);
					}
				}
				// if( replace ){
				// while( dictDescriptor.getFieldNumebr() >
				// replaceMark.length-1){
				// boolean[] newReplaceMark = new
				// boolean[(replaceMark.length<<1)];
				// System.arraycopy(replaceMark, 0, newReplaceMark, 0,
				// replaceMark.length);
				// replaceMark = newReplaceMark;
				// }
				// replaceMark[dictDescriptor.getFieldNumebr()]=true;
				// }
			} catch (IOException e) {
				LOG.error("get error while translate file:"
						+ localPath.toString());
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}
	}

	private Map<String, Path> buildLocalCacheFilesMap(Path[] uris) {
		Map<String, Path> map = new HashMap<String, Path>();
		if (uris == null) {
			return map;
		}
		for (Path uri : uris) {
			map.put(uri.getName(), uri);
		}
		return map;
	}

}