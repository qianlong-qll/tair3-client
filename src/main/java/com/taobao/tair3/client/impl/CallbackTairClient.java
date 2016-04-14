package com.taobao.tair3.client.impl;

/**
 * CallbackTairClient
 * @author tianmai.fh@taobao.com initial release
 * @date 2013-4-21
 */

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairQueueOverflow;
import com.taobao.tair3.client.TairBlockingQueue;
public class CallbackTairClient extends DefaultTairClient {
	protected long pollTime = 30L;
	protected long sleepTime = 20L;
	protected Thread pollerThread = null;

	public CallbackTairClient() {
		super();
	}

	public void init() throws TairException {
		super.init();
		startPoller();
	}

	public void close() {
		super.close();
		if (pollerThread != null) {
			pollerThread.interrupt();
			try {
				pollerThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	public void setPollTime(long pollTime) {
		this.pollTime = pollTime;
	}

	public void setSleepTime(long sleepTime) {
		this.sleepTime = sleepTime;
	}

	public CallbackTairClient(TairBlockingQueue queue) {
		this();
		this.setNotifyQueue(queue);
	}

	public static interface TairClientCallBack {
		public  void handle(Future<?> future, Object context);
	}

	protected static class TairClientCallbackWrapper {
		protected TairClientCallBack cb = null;
		protected Object context = null;
		public Object getContext() {
			return context;
		}
		public void setContext(Object context) {
			this.context = context;
		}
		public TairClientCallbackWrapper(TairClientCallBack cb, Object context) {
			this.cb = cb;
			this.context = context;
		}
	
		public void handle(NotifyFuture notifyFuture, Object context) {
			if (cb != null) {
				cb.handle(notifyFuture.getFuture(), context);
			}
		}
		
	}
	
	public void notifyFuture(Future<?> future, TairClientCallBack callback, Object context) throws TairQueueOverflow { 
		notifyFuture(future, new TairClientCallbackWrapper(callback, context));
	}

	/**
	* start the poller thread.
	*/
	private boolean startPoller = true;
	private void startPoller(){
		if(!this.startPoller){
			return;
		}
		pollerThread = new Thread(new Runnable(){
			public void run() {
				handle();
			}
		});
		pollerThread.start();
	}

	public void handle() {
		while (true) {
			try {
				NotifyFuture future = this.poll(pollTime, TimeUnit.SECONDS);
				if (future != null) {
					Object wrapper = future.getCtx();
					if (wrapper != null && wrapper instanceof TairClientCallbackWrapper) {
						TairClientCallbackWrapper cb = (TairClientCallbackWrapper) wrapper;
						Object context = cb.getContext();
						cb.handle(future, context);
					}
					else {
						//log error
					}
				} else {
					Thread.sleep(sleepTime);
				}
			} catch (Throwable e) {
				//log.error(e);
			}
		}
	}
}
