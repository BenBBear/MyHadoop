package cn.uc.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * <pre>
 * 使用此比较器，可以使reduce的输入按照字典序的正序排列。
 * 
 * 一般MR使用的比较器是基于整个字节的。而Text在序列化的时候，会使用长度作为头字节。
 * 所以使用默认的MR的比较器，较短长度的key会排在前面。
 * 使用此比较器，可以使reduce的输入按照字典序的正序排列。
 * </pre>
 * @author qiujw
 *
 */
public class TextSortComparator extends WritableComparator implements
		Configurable {
	protected TextSortComparator() {
		super(Text.class);
	}
	private Configuration conf;
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return conf;
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		int n1 = WritableUtils.decodeVIntSize(b1[s1]);
	    int n2 = WritableUtils.decodeVIntSize(b2[s2]);
		s1+=n1;l1-=n1;s2+=n2;l2-=n2;
		int t = Text.Comparator.compareBytes(b1, s1, l1, b2, s2, l2);
		return t;
	}

	// @Override
	public int compare(Text o1, Text o2) {
		return -(o1.compareTo(o2));
	}
}