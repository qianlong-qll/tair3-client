package com.taobao.tair3.client.error;

public class TairRpcError extends TairException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TairRpcError() {
		super();
	}

	public TairRpcError(String message, Throwable cause) {
		super(message, cause);
	}

	public TairRpcError(String message) {
		super(message);
	}

	public TairRpcError(Throwable cause) {
		super(cause);
	}
	
}
