package com.taobao.tair3.client.rpc.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.AbstractResponsePacket;
import com.taobao.tair3.client.packets.common.ReturnResponse;
import com.taobao.tair3.client.rpc.net.TairFuture;
import com.taobao.tair3.client.rpc.net.TairRpcPacket;
import com.taobao.tair3.client.rpc.net.TairFuture.TairFutureListener;
import com.taobao.tair3.client.TairBlockingQueue;


public class TairResultFutureImpl<S extends AbstractResponsePacket, T> extends TairResultFuture<T> {

	TairFuture impl;
	TairResultCast<S, T> cast;
	
	Class<S> retClst;
	private Object context = null;
	
	public TairResultFutureImpl(TairFuture impl, Class<S> retCls, 
							TairResultCast<S, T> cast, 
							Object context) {
		this.impl = impl;
		this.retClst = retCls;
		this.cast = cast;
		this.context = context;
		
	}

	class TairFutureListenerImpl implements TairFutureListener {
		final TairResultFuture<T> inst;
		final TairBlockingQueue queue;
		
		TairFutureListenerImpl(TairResultFuture<T> inst, TairBlockingQueue queue) {
			this.inst = inst;
			this.queue = queue;
		}

		public void handle(Future<TairRpcPacket> future) {
			queue.offer(inst);	
		}
	}

	public void futureNotify(final TairBlockingQueue queue) {
		setListener(new TairFutureListenerImpl(this, queue));
	}
	
	public void setListener(final TairFutureListener listener) {
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
		return innerGet();
	}
	
	public S getResponse() throws InterruptedException, ExecutionException {
		return innerGetResponse();
	}

	public T get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		return innerGet(timeout, unit);
	}
	
	protected S excutionException(Object response) {
		if (response instanceof ReturnResponse) {
			ReturnResponse r = (ReturnResponse) response;
			S t = null; 
			try {
				t = retClst.newInstance();
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
	
	protected S innerGetResponse() throws InterruptedException, ExecutionException {
		S retPacket = null;
		TairRpcPacket p = impl.get();
		if (p == null)
			throw new ExecutionException(new NullPointerException("futre<PacketWrapper> shouldn't return null"));
		try {
			retPacket = retClst.cast(p.getBody());
		} catch (ClassCastException e) {
			retPacket = excutionException(p.getBody());
		}
		return retPacket;
	}
	
	protected T innerGet() throws InterruptedException, ExecutionException {
		S retPacket = null;
		TairRpcPacket p = impl.get();
		if (p == null)
			throw new ExecutionException(new NullPointerException("futre<PacketWrapper> shouldn't return null"));
		try {
			retPacket = retClst.cast(p.getBody());
		} catch (ClassCastException e) {
			retPacket = excutionException(p.getBody());
		}
		
		if (retPacket == null)
			return null;
		try {
			return cast.cast(retPacket, context);
		} catch (TairRpcError e) {
			throw new ExecutionException(e);
		}
		catch (TairCastIllegalContext e) {
			throw new ExecutionException(e);
		}
	}

	protected T innerGet(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException,
			TimeoutException {
		S retPacket = null;
		TairRpcPacket p = impl.get(timeout, unit);
		if (p == null)
			throw new ExecutionException(new NullPointerException("futre<PacketWrapper> shouldn't return null"));
		try {
			retPacket =  retClst.cast(p.getBody());
		} catch (ClassCastException e) {
			retPacket = excutionException(p.getBody());
		}
		
		if (retPacket == null)
			return null;
		try {
			return cast.cast(retPacket, context);
		} catch (TairRpcError e) {
			throw new ExecutionException(e);
		}
		catch (TairCastIllegalContext e) {
			throw new ExecutionException(e);
		}
	}
}
