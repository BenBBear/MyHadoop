package cn.uc.hadoop.utils;

/**
 * 
 * @author zhaigy
 */
final public class Field {
	/**
	 * 
	 */
	private final Record recordExt;
	private Record subRecordExt;
	private int idx = 0;
	/** 用于用户设置值 */
	private String value;

	protected Field(Record recordExt) {
		this.recordExt = recordExt;
	}

	protected Field(Record recordExt, int idx) {
		this.recordExt = recordExt;
		this.idx = idx;
	}

	public void appendTo(StringBuffer sb) {
		if (value != null) {
			sb.append(value);
			return;
		}
		if (this.recordExt.fieldSize == 1) {// just one
			if (idx == 0) {
				sb.append(this.recordExt.record, this.recordExt.start,
						this.recordExt.end);
			}
			return;
		}
		int s = -1;
		int e = 1;
		sb.append("aaa".substring(s + 1, e));
		// for (int i = s + 1; i < e; ++i) {
		// sb.append(this.recordExt.record.charAt(i));
		// }
	}

	public void appendTo(StringBuilder sb) {
		if (value != null) {
			sb.append(value);
			return;
		}
		if (this.recordExt.fieldSize == 1) {// just one
			if (idx == 0) {
				sb.append(this.recordExt.record, this.recordExt.start,
						this.recordExt.end);
			}
			return;
		}
		int s = findStart();
		int e = findEnd();
		// sb.append(this.recordExt.record.toString(), s + 1, e);
		for (int i = s + 1; i < e; ++i) {
			sb.append(this.recordExt.record.charAt(i));
		}
	}

	public Record convertToRecordExt(char splitChar) {
		if (subRecordExt == null) {
			subRecordExt = new Record();
			int s = findStart();
			int e = findEnd();
			if (s < 0) {
				s = 0;
			}
			subRecordExt.setRecord(this.recordExt.record, s, e);
			subRecordExt.setSplitChar(splitChar);
		}
		return subRecordExt;
	}

	public boolean equals(CharSequence cs) {
		if (cs == null) {
			return false;
		}
		if (value != null) {
			return value.equals(cs);
		}
		int s = findStart();
		int e = findEnd();
		int len = e - s - 1;
		if (cs.length() != len) {
			return false;
		}
		for (int i = 0; i < len; ++i) {
			if (this.recordExt.record.charAt(s + 1 + i) != cs.charAt(i)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (value != null) {
			return value.equals(obj);
		}
		if (obj instanceof CharSequence) {
			CharSequence cs = (CharSequence) obj;
			return equals(cs);
		}
		return super.equals(obj);
	}

	/** 查找分隔符结束位置，注意是分隔符位置 ，如果分隔符在最后一位则返回字符串结束位置 */
	private int findEnd() {
		int e = idx == (this.recordExt.fieldSize - 1) ? this.recordExt.end
				: this.recordExt.splits[idx];
		return e;
	}

	/** 查找分隔符开始位置，注意是分隔符位置，如果分隔符在第一位则返回-1 */
	private int findStart() {
		int s = idx == 0 ? this.recordExt.start - 1
				: this.recordExt.splits[idx - 1];
		return s;
	}

	public void reset() {
		this.value = null;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		if (value != null) {
			return value;
		}
		int s = findStart();
		int e = findEnd();
		if (s >= e - 1) {
			return "";
		}
		return this.recordExt.record.substring(s + 1, e);
	}
}