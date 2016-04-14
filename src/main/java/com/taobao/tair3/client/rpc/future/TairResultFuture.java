package com.taobao.tair3.client.rpc.future;

import java.util.concurrent.Future;

import com.taobao.tair3.client.TairBlockingQueue;

public abstract class TairResultFuture<T> implements Future<T> {
	Object ctx;
	
	public void setContext(Object c) {
		this.ctx = c; 
	}
	
	public Object getContext() {
		return ctx;
	}
	
	public abstract void futureNotify(final TairBlockingQueue queue);
}