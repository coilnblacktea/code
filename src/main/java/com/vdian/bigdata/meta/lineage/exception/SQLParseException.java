package com.vdian.bigdata.meta.lineage.exception;
/**
 * @author yangyang
 */
public class SQLParseException extends RuntimeException {

	private static final long serialVersionUID = -5588025121452725145L;

	public SQLParseException(String message, Throwable cause) {
		super(message, cause);
	}

	public SQLParseException(String message) {
		super(message);
	}

	public SQLParseException(Throwable cause) {
		super(cause);
	}
}
