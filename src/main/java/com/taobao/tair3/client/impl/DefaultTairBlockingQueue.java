package com.taobao.tair3.client.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.taobao.tair3.client.rpc.future.TairResultFuture;
import com.taobao.tair3.client.TairBlockingQueue;

public class DefaultTairBlockingQueue implements TairBlockingQueue {
	protected BlockingQueue<TairResultFuture<?>> queue = null;
	
	public DefaultTairBlockingQueue() {
		queue = new LinkedBlockingQueue<TairResultFuture<?>>();
	}
	public TairResultFuture<?> poll() {
		return queue.poll();
	}
	
	public TairResultFuture<?> poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	public boolean offer(TairResultFuture<?> e) {
		return queue.offer(e);
	}

	public boolean offer(TairResultFuture<?> e, long timeout, TimeUnit unit) throws InterruptedException {
		return queue.offer(e, timeout, unit);
	}
	
	public void clear() {
		queue.clear();
	}
	
	public int size() {
		return queue.size();
	}

	public boolean add(TairResultFuture<?> e) {
		return queue.add(e);
	}
}
