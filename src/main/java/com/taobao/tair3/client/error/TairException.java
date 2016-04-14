package com.taobao.tair3.client.error;

import java.net.SocketAddress;

public class TairException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TairException() {
		super();
	}

	 
	public TairException(String message, Throwable cause) {
		super(message, cause);
	}

	public TairException(String message) {
		super(message);
	}

	public TairException(Throwable cause) {
		super(cause);
	}

}
