package cn.uc.hadoop.exception;

public class TextSplitIndexOutOfBoundsException extends IndexOutOfBoundsException {
	/**
	 * Constructs a <code>StringIndexOutOfBoundsException</code> with no detail
	 * message.
	 * 
	 * @since JDK1.0.
	 */
	public TextSplitIndexOutOfBoundsException() {
		super();
	}

	/**
	 * Constructs a <code>StringIndexOutOfBoundsException</code> with the
	 * specified detail message.
	 * 
	 * @param s
	 *            the detail message.
	 */
	public TextSplitIndexOutOfBoundsException(String s) {
		super(s);
	}

	/**
	 * Constructs a new <code>StringIndexOutOfBoundsException</code> class with
	 * an argument indicating the illegal index.
	 * 
	 * @param index
	 *            the illegal index.
	 */
	public TextSplitIndexOutOfBoundsException(int index) {
		super("text index out of range: " + index);
	}
}