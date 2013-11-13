package cn.uc.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.UTF8ByteArrayUtils;

/**
 * <pre>
 * 本函数用于Text的序列化byte数组 用于二次排序中的分组排序
 * 
 * 使用规则：map的key是 A+分隔符+B 会按照A的字典序比较,相同的会分入一个partition。
 * 
 * 使用规则：如果map中找不到key,将使用整个key用作比较。
 * 
 * 使用规则：map的key是 A+分隔符类型+B,会根据第一列的内容进行分区 1.设置分隔符
 * conf.set(TextFirstGroupComparator.TEXT_FIRST_GROUP_COMPATATOR,"``");
 * 2.设置partitioner的类 job.setPartitionerClass(TextFirstPartitioner.class);
 * job.setGroupingComparatorClass(TextFirstGroupComparator.class); 3.在map阶段的输出按照
 * A+分隔符类型+B的格式输出
 * </pre>
 * @author qiujw
 * 
 */
public class TextFirstGroupComparator extends WritableComparator implements
		Configurable {
	public TextFirstGroupComparator() {
		super(Text.class);
	}

	private Configuration conf;
	private byte[] split;
	public static String TEXT_FIRST_GROUP_COMPATATOR = TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR;
	private static final String TEXT_FIRST_GROUP_COMPATATOR_DEFAULT = TextFirstPartitioner.TEXT_FIRST_GROUP_COMPATATOR_DEFAULT;

	public void setConf(Configuration conf) {
		this.conf = conf;
		String splitString = conf.get(TEXT_FIRST_GROUP_COMPATATOR,
				TEXT_FIRST_GROUP_COMPATATOR_DEFAULT);
		split = splitString.getBytes();
	}

	public Configuration getConf() {
		return conf;
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {		
		int n1 = WritableUtils.decodeVIntSize(b1[s1]);
	    int n2 = WritableUtils.decodeVIntSize(b2[s2]);
		s1+=n1;l1-=n1;s2+=n2;l2-=n2;
		int p1 = UTF8ByteArrayUtils.findBytes(b1, s1, s1+l1, split);
		int p2 = UTF8ByteArrayUtils.findBytes(b2, s2, s2+l2 , split);
		//找不到split
		if( p1 != -1 ) l1 = p1 - s1;
		if( p2 != -1 ) l2 = p2 - s2;
		return WritableComparator
				.compareBytes(b1, s1, l1, b2, s2, l2);
	}

	// @Override
	public int compare(Text o1, Text o2) {
		return o1.compareTo(o2);
	}
	

}
