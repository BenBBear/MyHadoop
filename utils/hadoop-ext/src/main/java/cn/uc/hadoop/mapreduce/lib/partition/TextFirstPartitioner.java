package cn.uc.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.UTF8ByteArrayUtils;

/**
 *  <pre>
 * 本函数用于Text的第一列的分区函数 用于二次排序中的分区
 * 
 * 使用规则：map的key是 A+分隔符类型+B,会根据第一列的内容进行分区
 * 1.设置分隔符 conf.set(TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR,"``");
 * 2.设置partitioner的类 
 * 		job.setPartitionerClass(TextFirstPartitioner.class);
 * 		job.setGroupingComparatorClass(TextFirstGroupComparator.class);
 *      job.setSortComparatorClass(TextSortComparator.class);
 *      //job.setSortComparatorClass(ReverseTextSortComparator.class);
 * 3.在map阶段的输出按照 A+分隔符类型+B的格式输出
 * 
 * 例如：map阶段输出 如下KV对
 * ( a1`b1,v1   
 *   a2`b1,v2
 *   a11`b1,v3
 *   a2`b2,v2
 *   a1`b2,v1 
 *   a11`b2,v3)
 * 则在reduce阶段，将会得到如下的输入,(按照key的分隔符前面的部分分组，按照key的分隔符后面的部分字典序输入)
 * 
 * reduce-
 *   a1`b1,v1
 *   a1`b2,v1 
 * reduce-
 *   a11`b1,v3
 *   a11`b2,v3
 * reduce-
 *   a2`b1,v2
 *   a2`b2,v2
 * </pre>  
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