package com.taobao.tair3.client.rpc.net;

 
import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.taobao.eagleeye.EagleEye;
//import com.taobao.eagleeye.RpcContext_inner;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.error.TairAgain;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.rpc.net.TairRpcContext.FailCounter;
import com.taobao.tair3.client.rpc.protocol.tair2_3.PacketHeader;

public class TairFuture implements java.util.concurrent.Future<TairRpcPacket> {
	private ReentrantLock lock = new ReentrantLock();
	private Condition cond = lock.newCondition();
	private TairRpcPacket packet = null;
	private Throwable exception = null;
	private SocketAddress addr = null;
	private ChannelFuture connectFuture;
	//private RpcContext_inner eagleEyeContext;
	private FailCounter failCounter = null;
	private int waitCount = 0;
	
	TairFutureListener listener = null;
	
	public interface TairFutureListener {
		public void handle(Future<TairRpcPacket> future);
	}
    /*
	public void setEagleEyeContext(RpcContext_inner eagleEyeContext) {
		this.eagleEyeContext = eagleEyeContext;
	} 
    */
	public void setRemoteAddress(SocketAddress addr) {
		this.addr = addr;
	}
	public SocketAddress getRemoteAddress() {
		return this.addr;
	}
	public void setFailCounter(FailCounter failCounter) {
		this.failCounter = failCounter;
	}
	public void setConnectFuture(ChannelFuture connectFuture) {
		this.connectFuture = connectFuture;
		this.connectFuture.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {
				innerNotifyAll();
			}
		});
	}
	
	public void setValue(TairRpcPacket p) throws TairException {
		this.packet = p;
		
        /*
		// [EagleEye]
		if (eagleEyeContext != null && p != null) {
			ResultCode resultCode = ResultCode.castResultCode(p.decodeResultCode());
			eagleEyeContext.setResponseSize(PacketHeader.HEADER_SIZE + p.getBodyLength());
			eagleEyeContext.endRpc(String.valueOf(resultCode.errno()), EagleEye.TYPE_TAIR, null);
			EagleEye.commitRpcContext(eagleEyeContext);
		}
        */
		innerNotifyAll();
	}
	
	public boolean setException(Throwable r) {
		this.exception = r;
		if (failCounter != null) {
			failCounter.hadFail();
		}
		
        /*
		// [EagleEye]
		if (eagleEyeContext != null) {
			ResultCode errorCode = ResultCode.UNKNOWN;
			if (r instanceof TimeoutException) {
				errorCode = ResultCode.TIMEOUT;
			} 
			else if (r instanceof ConnectException) {
				errorCode = ResultCode.FAILED;
			}
			else {
				errorCode = ResultCode.UNKNOWN;
			}		
			eagleEyeContext.endRpc(String.valueOf(errorCode.errno()),
					EagleEye.TYPE_TAIR, null);
			EagleEye.commitRpcContext(eagleEyeContext);
		}
        */
		innerNotifyAll();
		return false;
	}
	
	public void setListener(TairFutureListener listener) {
		TairFutureListener nowCall = null;
		try {
			lock.lock();
			if (isDone()) {
				nowCall = listener;
			} else {
				this.listener = listener;
			}
		} finally {
			lock.unlock();
		}
		if (nowCall != null) {
			nowCall.handle(this);
		}
	}
	
	private void innerNotifyAll() {
		TairFutureListener nowCall = null;
		try {
			lock.lock();
			if (waitCount <= 1) {
				cond.signal();
			} else if (waitCount > 1) {
				cond.signalAll();
			}
			if (this.listener != null) {
				nowCall = this.listener;
				this.listener = null;
			}
		} finally {
			lock.unlock();
		}
		if (nowCall != null) {
			nowCall.handle(this);
		}
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		throw new UnsupportedOperationException();
	}

	public boolean isCancelled() {
		throw new UnsupportedOperationException();
	}

	public boolean isDone() {
		boolean connectDone = false;
		if (connectFuture != null)
			connectDone = connectFuture.isDone();
		
		return connectDone || (packet != null || exception != null);
	}
	
	private TairRpcPacket innerGet() throws ExecutionException {
		if (packet != null) {
			try {
				packet.decodeBody();
			} catch (TairException e) {
				String message = e.getMessage() + " remote: " + this.addr;
				throw new ExecutionException(message, e.getCause());
			}
			return packet;
		}
		
		if (exception != null) {
			throw new ExecutionException("remote: " + this.addr, exception);
		}
		
		if (connectFuture != null) {		
			if (connectFuture.getCause() != null)
				throw new ExecutionException("remote: " + this.addr, connectFuture.getCause());
			throw new ExecutionException("remote: " + this.addr, new TairAgain());
		}
		throw new ExecutionException(new Exception("no result had been set, remote: " + this.addr));
	}
	
	private void innerWait(long timeout, TimeUnit unit) throws InterruptedException {
		try {
			lock.lock();
			waitCount++;
			if (!isDone()) {
				if (timeout == -1)
					cond.await();
				else
					cond.await(timeout, unit);
			}
		} finally {
			waitCount--;
			lock.unlock();
		}
	}

	public TairRpcPacket get() throws InterruptedException, ExecutionException {
		if (!isDone()) {
			innerWait(-1, null);
		}
		return innerGet();
	}

	public TairRpcPacket get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		if (!isDone()) {
			innerWait(timeout, unit);
		}
		if (!isDone()) {
			throw new TimeoutException("Timeout, remote: " + this.addr);
		}
		return innerGet();
	}

}
