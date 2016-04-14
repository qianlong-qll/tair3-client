 	package com.taobao.tair3.client.rpc.net;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.rpc.net.FlowLimit.FlowStatus;


public class TairChannel {
	protected static final Logger log = LoggerFactory.getLogger(TairChannel.class);

	private TairConnector connector;
	private Channel channelImpl = null;
	private Throwable cause = null;

	private Object sessionLock = new Object();
	private Boolean finished = false;
	private SocketAddress destAddress;
	private TairRpcPacketFactory factory;
	private final static AtomicInteger channelSeq ;
	
	private ChannelFuture connectFuture;
	
	private AtomicInteger waitConnectCount = new AtomicInteger(0);
	
	private ConcurrentHashMap<Short, FlowLimit> flowLimitLevel = new ConcurrentHashMap<Short, FlowLimit>();
	
	static {
		channelSeq = new AtomicInteger(1);
	}
	
	private ConcurrentHashMap<Integer, TairFuture> tasks = new ConcurrentHashMap<Integer, TairFuture>();
	
	private TairRpcPacket cachedPacketWrapper;
	
	private WaitingChannelSeq waitingChannelSeq;
	
	public TairRpcPacket getCachedPacketWrapper() {
		return cachedPacketWrapper;
	}

	public void SetWaitingChannelSeq(WaitingChannelSeq waitingChannelSeq) {
		this.waitingChannelSeq = waitingChannelSeq;
	}
	
	public WaitingChannelSeq getWaitingChannelSeq() {
		return this.waitingChannelSeq;
	}
	
	public FlowLimit getFlowLimitLevel(short ns) {
		return flowLimitLevel.get(ns);
	}
	public int getCurrentThreshold(short ns) {
		FlowLimit fl = flowLimitLevel.get(ns);
		if (fl != null) {
			return fl.getThreshold();
		}
		return 0;
	}
	public void setCachedPacketWrapper(TairRpcPacket cachedPacketWrapper) {
		this.cachedPacketWrapper = cachedPacketWrapper;
	}
	
	public int incAndGetWaitConnectCount() {
		return waitConnectCount.getAndIncrement();
	}
	
	public int decAndGetWaitConnectCount() {
		return waitConnectCount.decrementAndGet();
	}
	
	public int getWaitConnectCount() {
		return waitConnectCount.get();
	}
	
	public int incAndGetChannelSeq() {
		int chid = channelSeq.getAndIncrement();
		if (chid == -1)
			return channelSeq.getAndIncrement();
		return chid;
	}

	public static TairChannel getTairChannel(Channel ctx)	 {
		return (TairChannel)ctx.getAttachment();
	}
	
	public TairRpcPacketFactory getPacketFactory() {
		return factory;
	}
	
	ChannelFutureListener ioFutureListener = new ChannelFutureListener() {
		
		public void operationComplete(ChannelFuture future) throws Exception {
			cause = future.getCause();
			channelImpl = future.getChannel();
			channelImpl.setAttachment(TairChannel.this);
			
			synchronized (sessionLock) {
				finished = true;
				sessionLock.notifyAll();
			}
		}
	};
	
	public TairChannel(TairConnector connector, SocketAddress destAddress, TairRpcPacketFactory factory) {
		this.connector 		= connector;
		this.destAddress	= destAddress;
		this.factory		= factory;
	}
	
	public ChannelFuture connect() {
		return connectFuture = connector.createSession(destAddress, ioFutureListener);
	}
	
	public ChannelFuture getConnectFuture() {
		return connectFuture;
	}
	
	public SocketAddress getDestAddress() {
		return destAddress;
	}
	
	public boolean isReady() {
		return channelImpl != null && cause == null;
	}
	
	public Throwable getCause() {
		return cause;
	}
	
	public boolean isTrafficDataOverflow(short ns) {
		if (ns <= 0) return false;
		
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null)
			return false;
		boolean ret = flowLimit.isOverflow();
		if (ret)
			log.debug("overflow threshold: " + flowLimit.getThreshold());
		return ret;
	}
	public boolean limitLevelUp(short ns) {
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null) {
			flowLimit = new FlowLimit(ns);
			flowLimitLevel.putIfAbsent(ns, flowLimit);
			flowLimit = flowLimitLevel.get(ns);
		} 
		boolean ret = flowLimit.limitLevelUp();
		log.warn("overflow threshold up: " + flowLimit.getThreshold());
		return ret;
	}
	
	public boolean limitLevelDown(short ns) {
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null) {
			return false;
		} 
		boolean ret = flowLimit.limitLevelDown();
	    log.warn("oveflow threshold down: " + flowLimit.getThreshold());
		return ret;
	}
	
	public void limitLevelTouch(short ns, FlowStatus status) {
		 
		switch (status) {
		case KEEP:
			limitLevelTouch(ns);
			break;
		case UP:
			limitLevelUp(ns);
			break;
		case DOWN:
			limitLevelDown(ns);
			break;
		default:
			break;
		}
	}
		
	public void limitLevelTouch(short ns) {
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null) {
			return ;
		} 
		flowLimit.limitLevelTouch();
	}
	
	public ChannelFuture sendPacket(final TairRpcPacket packet, final TairFuture rpcFuture) throws TairException {
		ChannelFuture future = channelImpl.write(packet.encode());
		if (rpcFuture != null) {
			future.addListener(new ChannelFutureListener() {
				public void operationComplete(ChannelFuture future)
						throws Exception {
					if (future.getCause() != null) {
						log.warn("send reuqest failed, cause:", future.getCause());
						rpcFuture.setException(future.getCause());
					} 
				}
			});
		}
		return future;
	}
	
	public <T> TairFuture registCallTask(int channelSeq) {
		TairFuture future = new TairFuture();
		future.setRemoteAddress(destAddress);
		tasks.put(channelSeq, future);
		return future;
	}
	
	public TairFuture getAndRemoveCallTask(int channelSeq) {
		TairFuture future = tasks.remove(channelSeq);
		return future;
	}
	
	public TairFuture clearTimeoutCallTask(int channelSeq) {
		TairFuture future = tasks.remove(channelSeq);
		if (future != null) {
			future.setException(new TairTimeout("waiting response timeout, remote: " + future.getRemoteAddress().toString()));
		}
		return future;
	}
	
	public boolean waitConnect(long waittime) {
		if (finished)
			return true;
		synchronized (sessionLock) {
			try {
				if (finished == false) {
					if (waittime == 0) {
						sessionLock.wait();
					} else {
						sessionLock.wait(waittime);
					}
				}
			} catch (InterruptedException e) {
				return false;
			}
			return finished;
		}
	}

	public void close() {
		if (channelImpl != null) {
			 try {
				channelImpl.close();
			 } catch (Exception e) {
			 	log.warn("close channel exception " + channelImpl, e);
			 }
		}
	}
}

