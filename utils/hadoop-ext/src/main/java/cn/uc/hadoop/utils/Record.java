package cn.uc.hadoop.utils;

/**
 * <pre>
 * 记录。可分为多个字段，使用特定字符分割。
 * 本类主要是为了提高效率，避免切分和产生中间对象。
 * 本类可复用。
 * </pre>
 * 
 * @author zhaigy
 */
final public class Record {
	private static int MAX_FIELD = 100;

	protected String record = "";
	protected int start;
	protected int end;

	protected char splitChar = '`';

	protected int[] splits;

	protected int fieldSize;
	protected Field[] fields;

	public Record() {
		reset();
	}

	/**
	 * 把所有字段按顺序装入
	 * 
	 * @param sb
	 */
	public void appendTo(StringBuilder sb) {
		splitIfNot();
		for (int i = 0; i < fieldSize; i++) {
			field(i).appendTo(sb);
			sb.append(splitChar);
		}
		if (sb.length() > 0) {
			sb.setLength(sb.length() - 1);
		}
	}

	/**
	 * 获取指定字段，支持负数，表示逆序，-1是倒数第一个。基于0。
	 * 
	 * @param i
	 * @return
	 */
	public Field field(int i) {
		splitIfNot();
		if (i < 0) {
			i = fieldSize + i;
		}
		if (i >= fieldSize) {
			return null;
		}
		if (fields == null) {
			fields = new Field[fieldSize + 10];
		} else if (fields.length < fieldSize) {
			Field[] temp = new Field[fieldSize + 10];
			System.arraycopy(fields, 0, temp, 0, fields.length);
			fields = temp;
		}
		if (fields[i] == null) {
			Field f = new Field(this, i);
			fields[i] = f;
		}
		return fields[i];
	}

	public int fieldSize() {
		splitIfNot();
		return fieldSize;
	}

	public int getEnd() {
		return end;
	}

	public final String getRecord() {
		if (start == 0 && end == record.length()) {
			return record;
		}
		return record.substring(start, end);
	}

	public final String getRecordAllData() {
		return record;
	}

	public final char getSplitChar() {
		return splitChar;
	}

	public int getStart() {
		return start;
	}

	public void reset() {
		fieldSize = 0;
		if (fields != null) {
			for (Field f : fields) {
				if (f != null) {
					f.reset();
				}
			}
		}
	}

	public final void setRecord(String record) {
		setRecord(record, 0, record.length());
	}

	public final void setRecord(String record, int start, int end) {
		this.record = record;
		this.start = start;
		this.end = end;
	}

	public final void setSplitChar(char splitChar) {
		this.splitChar = splitChar;
	}

	private void splitIfNot() {
		if (fieldSize > 0) {
			return;
		}
		fieldSize = 0;
		if (splits == null) {
			splits = new int[MAX_FIELD - 1];
		}
		for (int i = start; i < end; ++i) {
			if (record.charAt(i) == splitChar) {
				splits[fieldSize] = i;
				++fieldSize;
				if (fieldSize >= MAX_FIELD - 1) {
					break;
				}
			}
		}
		fieldSize += 1;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(1024 * 4);
		appendTo(sb);
		return sb.toString();
	}

}
