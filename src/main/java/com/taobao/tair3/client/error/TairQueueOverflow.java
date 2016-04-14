package com.taobao.tair3.client.error;

public class TairQueueOverflow extends TairException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TairQueueOverflow() {
		super();
	}

	public TairQueueOverflow(String message, Throwable cause) {
		super(message, cause);
	}

	public TairQueueOverflow(String message) {
		super(message);
	}

	public TairQueueOverflow(Throwable cause) {
		super(cause);
	}
}
