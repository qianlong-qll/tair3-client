package com.taobao.tair3.client;

import java.util.concurrent.TimeUnit;

import com.taobao.tair3.client.rpc.future.TairResultFuture;

public interface TairBlockingQueue {
	public TairResultFuture<?> poll();
	public TairResultFuture<?> poll(long timeout, TimeUnit unit) throws InterruptedException;
	public boolean offer(TairResultFuture<?> e);
	public boolean offer(TairResultFuture<?> e, long timeout, TimeUnit unit) throws InterruptedException;
	public void clear();
	public int size();
}
