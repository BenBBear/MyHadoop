package cn.uc.hadoop.utils;

import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.Text;

import cn.uc.hadoop.exception.TextSplitIndexOutOfBoundsException;

/**
 * <pre>
 * 本TextRecord设计为和Record.class类似。 使用前设置split，并传入Text进行打散保存。
 * 然后，使用getField进行处理或者替换。 最后，使用getRecord获取整个Text文本.
 * 
 * 经典使用方式: <code>
 * TextRecord tr = new TextRecord();//创建对象
 * tr.setSplit("`");//设置分隔符
 * ...
 * Text temp = new Text("abcd`efg");
 * tr.reset(temp);//将其他的Text存入record进行打散
 * Text t0 = tr.getField(0);//获取第0个Text并使用
 * Text t1 = tr.getField(1);//获取第1个Text并使用
 * tr.getField(0).set("a");//修改第0个Text。请注意，由于getField返回的是和tr数组中的text的同一个引用
 * t0.set("a");            //所以修改t0也会影响tr的内容。所以，修改t0和修改tr.getField(0)的含义是一样的
 * tr.append("hi");          //追加一个Text
 * Text returnText = tr.getRecord();//组合为最终的Text,returnText的值为a`efg`hi
 * ...
 * tr.reset(other);//处理下一个text
 * 
 * </code>
 * 
 * 什么时候使用TextRecord，什么时候使用TextUtils? 答： TextUtils
 * 是无状态的调用，输出的结果，仅依赖输入。使用比较简单，坑比较少。 但是，如果需要获取多个字段，需要多次遍历这个Text，性能较低。 TextRecord
 * 包括split和内部的Text数组，都是有状态的。 一开始，针对字节数组进行全部打散并保存。
 * 如果，同时处理不同的Text需要创建多个TextRecord。
 * 通过getFeild获取Text，要注意此引用和原来的TextRecord中的Text输关联的。
 * 
 * 推荐： 如果需要对一个记录进行5次以上的的字段获取等操作的时候，使用TextRecord，可获得更好的性能。 否则，使用TextUtils
 * 
 * </pre>
 * 
 * @author qiujw
 * 
 */
public class TextRecord {

	protected int maxLength = 128;
	protected Text[] tArray;
	protected byte[] split;
	protected int length;

	/**
	 * 构建一个TextRecord
	 */
	public TextRecord() {
		tArray = new Text[maxLength];
		reset();
	}

	private void expandArray() {
		int newLength = maxLength << 1;
		Text[] newArray = new Text[newLength];
		System.arraycopy(tArray, 0, newArray, 0, maxLength);
		maxLength = newLength;
		tArray = newArray;
	}

	/**
	 * 在TextRecord后面添加一个字段，字段的内容是一个字符
	 * 
	 * @param c
	 *            要添加的字符
	 */
	public void append(char c) throws CharacterCodingException {
		Text temp = new Text();
		temp.set(TextUtils.encode(c));
		append(temp);
	}

	/**
	 * 在TextRecord后面添加一个字段，字段的内容是一个字符串
	 * 
	 * @param s
	 *            要添加的字符串
	 */
	public void append(String s) {
		append(new Text(s));
	}

	/**
	 * 在TextRecord后面添加一个字段，字段的内容是一个Text 注意，这种添加是引用的添加。 所以，外部的text修改会导致内部的修改。 例如:
	 * tr.reset(); Text t = new Text("a"); tr.append(t);
	 * tr.getRecord();//这时会返回一个a t.set("b"); tr.getRecord();//这时会返回一个b
	 * 
	 * @param t
	 *            要添加的Text字符串
	 */
	public void append(Text t) {
		if (length == maxLength) {
			expandArray();
		}
		tArray[length] = t;
		length++;
	}

	/**
	 * 获取指定字段，支持负数，表示逆序，-1是倒数第一个。基于0。
	 * 
	 * @param i
	 *            要获取的字段的下标
	 * @return 返回该字段的引用
	 */
	public Text field(int i) {
		return getField(i);
	}

	/**
	 * 获取指定字段，支持负数，表示逆序，-1是倒数第一个。基于0。
	 * 
	 * @param i
	 *            要获取的字段的下标
	 * @return 返回该字段的引用
	 */
	public Text getField(int i) {
		if (i < -length) {
			throw new TextSplitIndexOutOfBoundsException(i);
		}

		if (i < 0) {
			i = length + i;
		}

		if (i >= length) {
			throw new TextSplitIndexOutOfBoundsException(i);
		}

		return tArray[i];
	}

	/**
	 * 获取当前的TextRecord的字段数量
	 * 
	 * @return 字段数量
	 */
	public int fieldSize() {
		return length;
	}

	/**
	 * 将当前的TextRecord的字段使用分隔符拼接为一个Text返回。
	 * 
	 * @return 拼接后的Text
	 */
	public Text getRecord() {
		return TextUtils.join(tArray, length, split);
	}

	/**
	 * 将当前的TextRecord的字段使用分隔符拼接为一个Text返回。
	 * 
	 * @return 拼接后的Text
	 */
	public Text getRecordAllData() {
		return TextUtils.join(tArray, length, split);
	}

	/**
	 * 设置分隔符,分隔符会保存在TextRecord内部，在创建对象后设置一次即可。
	 * 
	 * @param s
	 *            分隔符
	 * @throws CharacterCodingException
	 */
	public void setSplit(String s) throws CharacterCodingException {
		split = TextUtils.encode(s);
	}

	/**
	 * 设置分隔符,分隔符会保存在TextRecord内部，在创建对象后设置一次即可。
	 * 
	 * @param c
	 *            分隔符
	 * @throws CharacterCodingException
	 */
	public void setSplit(char c) throws CharacterCodingException {
		split = TextUtils.encode(c);
	}

	/**
	 * 设置分隔符,分隔符会保存在TextRecord内部，在创建对象后设置一次即可。
	 * 
	 * @param b
	 *            分隔符
	 * @throws CharacterCodingException
	 */
	public void setSplit(byte[] b) {
		split = new byte[b.length];
		System.arraycopy(b, 0, this.split, 0, b.length);
	}

	/**
	 * 获取分隔符
	 * 
	 * @return 获取分隔符
	 */
	public byte[] getSplit() {
		return split;
	}

	/**
	 * 重置TextRecord
	 */
	public void reset() {
		reset(null);
	}

	/**
	 * 使用Text，重置TextRecord TextRecord将会打散该Text并保存
	 */
	public void reset(Text text) {
		if (text == null) {
			length = 0;
			return;
		}
		// TODO 使用静态数组?
		// 采集分割后的下标,如果下标超出maxlength，将复制数组，拓展大小到原来的2倍
		length = 0;

		byte[] b = text.getBytes();
		int bLength = text.getLength();
		int pos = -1;
		int nextStart = 0;
		do {
			pos = BytesUtils.findBytes(b, nextStart, bLength, split);
			if (pos >= 0) {
				Text temp = new Text();
				temp.set(b, nextStart, pos - nextStart);
				append(temp);
			} else {
				Text temp = new Text();
				temp.set(b, nextStart, bLength - nextStart);
				append(temp);
			}
			nextStart = pos + split.length;
		} while (pos >= 0);
		// 复制字节到数组中
	}
	/**
	 * 寻找目标字符串所在的字段的下标
	 * @param s 目标字符串
	 * @return 如果找到,返回字段下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public int find(String s) throws CharacterCodingException {
		return find(TextUtils.encode(s));
	}
	/**
	 * 寻找目标字节数组所在的字段的下标
	 * @param b 字节数组
	 * @return 如果找到,返回字段下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public int find(byte[] b) {
		int i, j;
		for (i = 0; i < length; i++) {
			if (tArray[i].getLength() == b.length) {
				for (j = 0; j < b.length; j++) {
					byte[] temp = tArray[i].getBytes();
					if (temp[j] != b[j]) {
						break;
					}
				}
				if (j == b.length)
					return i;
			}
		}
		return -1;
	}
	/**
	 * 寻找目标Text所在的字段的下标
	 * @param t 目标Text
	 * @return 如果找到,返回字段下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public int find(Text t) {
		int i;
		for (i = 0; i < length; i++) {
			if(tArray[i].equals(t)) return i;
		}
		return -1;
	}

	@Override
	public String toString() {
		return getRecord().toString();
	}
}
