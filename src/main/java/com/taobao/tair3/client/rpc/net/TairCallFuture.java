package com.taobao.tair3.client.rpc.net;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.taobao.tair3.client.packets.AbstractResponsePacket;
import com.taobao.tair3.client.packets.common.ReturnResponse;
import com.taobao.tair3.client.rpc.net.TairFuture.TairFutureListener;



public class TairCallFuture<T extends AbstractResponsePacket> implements Future<T> {
	
	TairFuture impl;
	Class<T> retCls;
	
	protected T excutionException(Object response) {
		if (response instanceof ReturnResponse) {
			ReturnResponse r = (ReturnResponse) response;
			T t = null; 
			try {
				t = retCls.newInstance();
			} catch (InstantiationException e1) {
				e1.printStackTrace();
			} catch (IllegalAccessException e1) {
				e1.printStackTrace();
			}
			if (t != null) {
				t.setCode(r.getCode());
			}
			return t;
		}
		//never return null;
		return null;
	}
	public TairCallFuture(TairFuture impl, Class<T> retCls) {
		this.impl = impl;
		this.retCls = retCls;
	}
		
	public void setListener(TairFutureListener listener) {
		impl.setListener(listener);
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		return impl.cancel(mayInterruptIfRunning);
	}

	public boolean isCancelled() {
		return impl.isCancelled();
	}

	public boolean isDone() {
		return impl.isDone();
	}

	public T get() throws InterruptedException, ExecutionException {
		TairRpcPacket p = impl.get();
		if (p == null)
			throw new ExecutionException(new NullPointerException("futre<PacketWrapper> shouldn't return null"));
		try {
			return retCls.cast(p.getBody());
		} catch (ClassCastException e) {
			return excutionException(p.getBody());
		}
	}

	public T get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException,
			TimeoutException {
		TairRpcPacket p = impl.get(timeout, unit);
		if (p == null)
			throw new ExecutionException(new NullPointerException("futre<PacketWrapper> shouldn't return null"));
		try {
			return retCls.cast(p.getBody());
		} catch (ClassCastException e) {
			return excutionException(p.getBody());
		}
	}
	
}