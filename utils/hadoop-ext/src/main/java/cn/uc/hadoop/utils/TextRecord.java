package cn.uc.hadoop.utils;

import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.Text;

import cn.uc.hadoop.exception.TextSplitIndexOutOfBoundsException;

public class TextRecord {

	protected int maxLength = 128;
	protected Text[] tArray;
	protected byte[] split;
	protected int length;

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
	 * 追加string char 或者 text到数组最后 其中string 和char 是会进行复制的
	 * text不会，所以，外部的text修改会导致内部的修改
	 */
	public void append(char c) throws CharacterCodingException {
		Text temp = new Text();
		temp.set(TextUtils.encode(c));
		append(temp);
	}

	public void append(String s) {
		append(new Text(s));
	}

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
	 * @return
	 */
	public Text field(int i) {
		return getField(i);
	}

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

	public int fieldSize() {
		return length;
	}

	public Text getRecord() {
		return TextUtils.join(tArray, length, split);
	}

	public Text getRecordAllData() {
		return TextUtils.join(tArray, length, split);
	}

	public void setSplit(String s) throws CharacterCodingException {
		split = TextUtils.encode(s);
	}

	public void setSplit(char s) throws CharacterCodingException {
		split = TextUtils.encode(s);
	}

	public void setSplit(byte[] s) {
		split = new byte[s.length];
		System.arraycopy(s, 0, this.split, 0, s.length);
	}

	public byte[] getSplit() {
		return split;
	}

	public void reset() {
		reset(null);
	}

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
				temp.set(b, nextStart, pos-nextStart);
				append(temp);
			} else {
				Text temp = new Text();
				temp.set(b, nextStart, bLength-nextStart);
				append(temp);
			}
			nextStart = pos + split.length;
		} while (pos >= 0);
		// 复制字节到数组中
	}

	@Override
	public String toString() {
		return getRecord().toString();
	}
}
