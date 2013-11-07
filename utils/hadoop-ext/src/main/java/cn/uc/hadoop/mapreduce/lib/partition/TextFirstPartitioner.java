package cn.uc.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.UTF8ByteArrayUtils;

/**
 * 
 * 本函数用于Text的第一列的分区函数 用于二次排序中的分区
 * 
 * 使用规则：map的key是 A+分隔符类型+B,会根据第一列的内容进行分区
 * 1.设置分隔符 conf.set(TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR,"``");
 * 2.设置partitioner的类 
 * 		job.setPartitionerClass(TextFirstPartitioner.class);
 * 		job.setGroupingComparatorClass(TextFirstGroupComparator.class);
 * 3.在map阶段的输出按照 A+分隔符类型+B的格式输出
 * 
 * @author qiujw
 * 
 */
public class TextFirstPartitioner extends Partitioner<Text, Text> implements
		Configurable {

	private Configuration conf;
	private byte[] split;
	public static String TEXT_FIRST_GROUP_COMPATATOR = "mapreduce.text.key.field.separator";
	public static final String TEXT_FIRST_GROUP_COMPATATOR_DEFAULT = "``";

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		String splitString = conf.get(TEXT_FIRST_GROUP_COMPATATOR,
				TEXT_FIRST_GROUP_COMPATATOR_DEFAULT);
		split = splitString.getBytes();
	}

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		byte[] k = key.getBytes();
		int klength = key.getLength();
		if (k == null || klength == 0)
			return 0;
		int pos = UTF8ByteArrayUtils.findBytes(k, 0, klength, split);
		int hashcode = 0;
		if (pos == -1) {
			hashcode = MyHashBytes(k, 0, klength);
		} else {
			hashcode = MyHashBytes(k, 0, pos);
		}
		return (hashcode & Integer.MAX_VALUE) % numPartitions;
	}
	
	//使用大质数代替31增强hash性
	public static int MyHashBytes(byte[] bytes, int offset, int length) {
	    int hash = 1;
	    for (int i = offset; i < offset + length; i++)
	      hash = (95549 * hash) + (int)bytes[i];
	    return hash;
	  }
}