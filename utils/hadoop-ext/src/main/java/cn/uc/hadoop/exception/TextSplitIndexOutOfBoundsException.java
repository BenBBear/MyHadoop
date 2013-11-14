package cn.uc.hadoop.exception;

public class TextSplitIndexOutOfBoundsException extends IndexOutOfBoundsException {
	/**
	 * Constructs a <code>TextSplitIndexOutOfBoundsException</code> with no detail
	 * message.
	 * 
	 */
	public TextSplitIndexOutOfBoundsException() {
		super();
	}

	/**
	 * Constructs a <code>TextSplitIndexOutOfBoundsException</code> with the
	 * specified detail message.
	 * 
	 * @param s
	 *            the detail message.
	 */
	public TextSplitIndexOutOfBoundsException(String s) {
		super(s);
	}

	/**
	 * Constructs a new <code>TextSplitIndexOutOfBoundsException</code> class with
	 * an argument indicating the illegal index.
	 * 
	 * @param index
	 *            the illegal index.
	 */
	public TextSplitIndexOutOfBoundsException(int index) {
		super("text index out of range: " + index);
	}
}