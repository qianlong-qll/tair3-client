package com.taobao.tair3.client.error;

public class TairServerReject extends TairException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TairServerReject() {
		super();
	}

	public TairServerReject(String message, Throwable cause) {
		super(message, cause);
	}

	public TairServerReject(String message) {
		super(message);
	}

	public TairServerReject(Throwable cause) {
		super(cause);
	}
}
