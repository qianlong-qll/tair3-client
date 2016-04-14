package com.taobao.tair3.client.rpc.net;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.taobao.eagleeye.EagleEye;
//import com.taobao.eagleeye.RpcContext_inner;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.ServerManager;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.impl.invalid.InvalidServer;
import com.taobao.tair3.client.packets.AbstractPacket;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.packets.AbstractResponsePacket;
import com.taobao.tair3.client.packets.dataserver.TrafficCheckResponse;
import com.taobao.tair3.client.rpc.future.TairResultFutureImpl;
import com.taobao.tair3.client.rpc.protocol.tair2_3.PacketHeader;
import com.taobao.tair3.client.rpc.protocol.tair2_3.PacketManager;


public class TairRpcContext {
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private ReentrantReadWriteLock channelMapLock = new ReentrantReadWriteLock();

	//this map was protected by rwLock 
	private ConcurrentHashMap<SocketAddress, TairChannel> channelMap = new ConcurrentHashMap<SocketAddress, TairChannel>();

	private TairConnector connector = null;
	private TairBgWorker  bgWorker = null;
	private ServerManager serverManager = null;
	private String group = null;
	class FailCounter {
		
		protected ServerManager serverManager = null;
		protected int maxFailCount = 100;
	    protected AtomicInteger failCounter = new AtomicInteger(0);
	    public FailCounter(ServerManager serverManager) {
	    	this.serverManager = serverManager;
	    }
	    public FailCounter() {
	    }
	    public void setServerManager(ServerManager serverManager) {
	    	this.serverManager = serverManager;
	    }
	    public void setMaxFailCount(int maxFailedCount) {
	    	this.maxFailCount = maxFailedCount;
	    }
	    public int getMaxFailCount() {
	    	return this.maxFailCount;
	    }
	    public void hadFail() {
	    	if (failCounter.incrementAndGet() >= this.maxFailCount) {
	    		if (serverManager != null) {
	    			serverManager.checkVersion(1);
	    		}
	    		failCounter.set(0);
	    	}
	    }
	}
	private FailCounter failCounter = null;

	public TairRpcContext(NioClientSocketChannelFactory nioFactory, TairBgWorker bgWorker, String groupName) {
		connector = new TairConnector(this, nioFactory);
		this.bgWorker = bgWorker;
		this.group = groupName;
		failCounter = new FailCounter();
	}
	public void setServerManager(ServerManager serverManager) {
		this.serverManager = serverManager;
		this.failCounter.setServerManager(serverManager);
		this.group = this.serverManager.getRowGroupName();
	}
	public void setMaxFailCount(int maxFailCount) {
		if (failCounter != null) {
			failCounter.setMaxFailCount(maxFailCount);
		}
	}
	public int getMaxFailCount() {
		if (failCounter != null) {
			return failCounter.getMaxFailCount();
		}
		return 0;
	}
	private void deleteSession(TairChannel channel) {
		if (channelMap.get(channel.getDestAddress()) == channel) {
			// double check removed channel  
			TairChannel deleteChannel = null;
			try {
				channelMapLock.writeLock().lock();
				if (channelMap.get(channel.getDestAddress()) == channel)
					deleteChannel = channelMap.remove(channel.getDestAddress());

			} finally {
				channelMapLock.writeLock().unlock();
			}
			if (deleteChannel != null)
				deleteChannel.close();
			log.info("GroupName: " + this.group + ", channel[id]: " + channel.getDestAddress() + "has be removed.");
		}
	}

	private TairChannel obtainSession(SocketAddress addr, TairRpcPacketFactory factory, long waittime) throws TairRpcError {

		TairChannel existChannel = null;
		try {
			channelMapLock.readLock().lock();
			existChannel = channelMap.get(addr);
			if (existChannel == null) {
				TairChannel channel = new TairChannel(connector, addr, factory);
				existChannel = channelMap.putIfAbsent(addr, channel);
				if (existChannel == null) {
					existChannel = channel;
					existChannel.connect();
				}
			}
		} finally {
			channelMapLock.readLock().unlock();
		}
		//fix bug, should delete the session
		if (existChannel.getCause() != null) {
			deleteSession(existChannel);
			throw new TairRpcError("GroupName: " + this.group + ", get channel failed:" + addr, existChannel.getCause());
		}

		if (existChannel.isReady() || waittime == 0) {		
			return existChannel;
		} else {
			if (existChannel.waitConnect(waittime)) {
				if (existChannel.getCause() != null) {
					throw new TairRpcError("GroupName: " + this.group + ", connect [ip]:" + addr, existChannel.getCause());
				} else if (existChannel.isReady()) {
					return existChannel;
				} else {
					// Never reach here
					log.error("GroupName: " + this.group + ", why reach here? target[ip]: "+ addr.toString());
				}
			} 
			log.info("GroupName: " + this.group + ", create tair connection success, target[ip]: "+ addr.toString());
			return null;
		}
	}

	private void deleteSession(Channel ch) {
		TairChannel channel = TairChannel.getTairChannel(ch);
		if (channel != null) {
			deleteSession(channel);
		}
		else {	
			log.warn("GroupName: " + this.group + " delete session with: null channel, connection: " + ch.getRemoteAddress());
		}
	}

	/*
	 * return null, timeout
	 */
	//for ds
	public <PacketT> TairFuture callAsync(final TairChannel channel,
			PacketT request, long timeout, TairRpcPacketFactory factory)
			throws TairRpcError {

		// step 1, get session
		// final TairChannel channel = obtainSession(addr, factory, 0);
		// if (channel == null)
		// throw new TairRpcError("aync call failed, session not created");

		// step 2, send request
		// step 2.1, build PacketWrapper and register packet

		final TairRpcPacket requestWrapper = factory.buildWithBody(channel.incAndGetChannelSeq(), request);

		bgWorker.addWaitingChannelSeq(channel, requestWrapper.getChannelSeq(), timeout);
		final TairFuture tairFuture = channel.registCallTask(requestWrapper.getChannelSeq());
		if (tairFuture == null) {
			throw new TairRpcError("duplicate channel id, GroupName: " + this.group + ", remote: " + channel.getDestAddress().toString());
		}
		//set `failCounter
		tairFuture.setFailCounter(failCounter);
		ChannelFutureListener listener = new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future) throws Exception {
				// step 2.1, send
				if (future != null) {
					channel.decAndGetWaitConnectCount();
					if (future.getCause() != null) {
						tairFuture.setException(future.getCause());
						bgWorker.removeWaitingChannelSeq(channel.getWaitingChannelSeq());
						channel.getAndRemoveCallTask(requestWrapper.getChannelSeq());
						return;
					}
					channel.waitConnect(0);
				}
				try {
					channel.sendPacket(requestWrapper, tairFuture);
				} catch (TairException e) {
					serverManager.maybeForceCheckVersion();
					throw new TairRpcError(e.getMessage() + ", remote: " + channel.getDestAddress().toString(), e.getCause());

				}
			}
		};

        /*
		// [EagleEye]
		if (request instanceof AbstractRequestPacket) {
			AbstractRequestPacket reqPacket = (AbstractRequestPacket) request;
			if (reqPacket.getNamespace() != 0) {
				EagleEye.startRpc(String.valueOf(PacketManager.getPacketCode(reqPacket.getClass())), serverManager.getRowGroupName());
				RpcContext_inner rpcContext = EagleEye.popRpcContext();
				tairFuture.setEagleEyeContext(rpcContext);
				rpcContext.setRemoteIp(channel.getDestAddress().toString());
				rpcContext.setRequestSize(PacketHeader.HEADER_SIZE + reqPacket.size());
				rpcContext.setRemoteIp(channel.getDestAddress().toString());
				rpcContext.setCallBackMsg(String.valueOf(reqPacket.getNamespace()));
				rpcContext.rpcClientSend();
			}
		}
        */
		if (channel.isReady() == false) {
			ChannelFuture connectFuture = channel.getConnectFuture();
			if (connectFuture == null) {
				serverManager.maybeForceCheckVersion();
				throw new TairRpcError("concurrent error, GroupName: " + this.group + ", remote: " + channel.getDestAddress().toString());
			}
			if (channel.getWaitConnectCount() > 128) {
				tairFuture.setConnectFuture(connectFuture);
			} else {
				channel.incAndGetWaitConnectCount();
				connectFuture.addListener(listener);
			}
		} else {
			try {
				listener.operationComplete(null);
			} catch (Exception e) {
				serverManager.maybeForceCheckVersion();
				throw new TairRpcError(e.getMessage() + "GroupName: " + this.group + ", remote: " + channel.getDestAddress().toString(), e.getCause());
			}
		}

		return tairFuture;
	}

	public <PacketT extends AbstractRequestPacket, S extends AbstractResponsePacket, T> TairResultFutureImpl<S, Result<T>> callAsync(
			SocketAddress addr, PacketT request, final long timeout,
			Class<S> retCls, TairRpcPacketFactory factory,
			TairResultCast<S, Result<T>> cast) throws TairRpcError, TairFlowLimit {
		final TairChannel channel = obtainSession(addr, factory, 0);
		if (channel == null) {
			TairRpcError e = new TairRpcError("aync call failed, session not created, GroupName: " + this.group + ", Area: " + request.getNamespace() + ",  remote: " + addr.toString());
//			recordExceptionToEagleeye(ResultCode.FAILED, request.getNamespace(), PacketManager.getPacketCode(request.getClass()), e, addr);
			throw e; 
		}

		checkLevelDown(channel, factory, request.getNamespace());
		if (channel.isTrafficDataOverflow(request.getNamespace())) {
			TairFlowLimit e = new TairFlowLimit("rpc overflow, GroupName: " + this.group + ", Area: " + request.getNamespace() + ", remote: " + channel.getDestAddress().toString());
//			recordExceptionToEagleeye(ResultCode.RPC_OVERFLOW, request.getNamespace(), PacketManager.getPacketCode(request.getClass()), e, addr);
			throw e;
		}
		TairFuture future = this.callAsync(channel, request, timeout, factory);
		return new TairResultFutureImpl<S, Result<T>>(future, retCls, cast, request.getContext());
	}
	public <PacketT extends AbstractRequestPacket, S extends AbstractResponsePacket, T> TairResultFutureImpl<S, Result<T>> callInvalServerAsync(
			InvalidServer ivs, PacketT request, final long timeout,
			Class<S> retCls, TairRpcPacketFactory factory,
			TairResultCast<S, Result<T>> cast) throws TairRpcError, TairFlowLimit {
		SocketAddress addr = ivs.getAddress();
		final TairChannel channel = obtainSession(addr, factory, 0);
		if (channel == null) {
			TairRpcError e = new TairRpcError("aync call failed, session not created, GroupName: " + this.group + ", Area: " + request.getNamespace() + ",  remote: " + addr.toString());
//			recordExceptionToEagleeye(ResultCode.FAILED, request.getNamespace(), PacketManager.getPacketCode(request.getClass()), e, addr);
			throw e; 
		}

		TairFuture future = this.callAsync(channel, request, timeout, factory);
		return new TairResultFutureImpl<S, Result<T>>(future, retCls, cast, request.getContext());
	}

	public <PacketT extends AbstractPacket, S extends AbstractResponsePacket, T> TairResultFutureImpl<S, Result<T>> callAsyncUnlimit(
			SocketAddress addr, PacketT request, final long timeout,
			Class<S> retCls, TairRpcPacketFactory factory,
			TairResultCast<S, Result<T>> cast) throws TairRpcError,
			TairFlowLimit {
		final TairChannel channel = obtainSession(addr, factory, 0);
		if (channel == null) {
			throw new TairRpcError("aync call failed, session not created, GroupName: " + this.group + ", remote: " + addr.toString());
		}
		TairFuture future = this.callAsync(channel, request, timeout, factory);
		return new TairResultFutureImpl<S, Result<T>>(future, retCls, cast, request.getContext());
	}

	public void messageReceived(Channel channel, TairRpcPacket packet) throws Exception {
		TairChannel tairChannel = TairChannel.getTairChannel(channel);
		
		if (packet.getChannelSeq() == -1) {
			packet.decodeBody();
			if (packet.getBody() instanceof TrafficCheckResponse) {
				TrafficCheckResponse tcr = (TrafficCheckResponse) packet.getBody();
				tairChannel.limitLevelTouch(tcr.getNamespace(), tcr.getStatus());
                int threshold = tairChannel.getCurrentThreshold(tcr.getNamespace());
                /*
                // [EagleEye]
                EagleEye.startRpc(String.valueOf(PacketManager.getPacketCode(tcr.getClass())), serverManager.getRowGroupName());
                RpcContext_inner rpcContext = EagleEye.popRpcContext();
                rpcContext.setRemoteIp(tairChannel.getDestAddress().toString()); 
                rpcContext.setCallBackMsg(String.valueOf(tcr.getNamespace()));
                String msg = tcr.getStatus().toString() + "," + threshold;
                rpcContext.endRpc(msg, EagleEye.TYPE_TAIR, null);
                rpcContext.rpcClientSend();
                EagleEye.commitRpcContext(rpcContext);
                */	
			} else {
				log.warn("GroupName: " + this.group + ", from the remote: " + channel.getRemoteAddress().toString() + " with seq=-1 packet, class: " + packet.getBody().getClass());
			}

		} else {
			bgWorker.removeWaitingChannelSeq(tairChannel.getWaitingChannelSeq());
			TairFuture future = tairChannel.getAndRemoveCallTask(packet.getChannelSeq());
			if (future == null)
				return;

			if (packet.hasConfigVersion()) {
				serverManager.checkVersion(packet.decodeConfigVersion());
			}
			future.setValue(packet);
		}
		

	}

	public void exceptionCaught(Channel channel, Throwable cause)
		throws Exception {
			log.warn("exception, will close connection", cause);
			deleteSession(channel);
		}

	public void channelDisconnected(Channel channel) {
		if (channel != null) {
			SocketAddress local = channel.getLocalAddress();
			SocketAddress remote = channel.getRemoteAddress();
			log.info("GroupName: " + this.group + ", LINK: [" + local.toString() + ", " + remote.toString() + " ] was disconnected by remote peer.");
			deleteSession(channel);
		}
	}

	protected void checkLevelDown(TairChannel channel, TairRpcPacketFactory factory, short ns) throws TairRpcError {
		FlowLimit flowLimit = channel.getFlowLimitLevel(ns);
		if (flowLimit == null) {
			return ;
		} 
		flowLimit.limitLevelCheck(this, factory, channel, ns);
	}

    /*
	public void recordExceptionToEagleeye(ResultCode rc, short namespace,  int pcode, Exception e, SocketAddress addr) {
		if (namespace != 0) {
			EagleEye.startRpc(String.valueOf(pcode), this.group);
            RpcContext_inner rpcContext = EagleEye.popRpcContext();
            if (addr != null)
            	rpcContext.setRemoteIp(addr.toString());
            rpcContext.endRpc(String.valueOf(rc.errno()), EagleEye.TYPE_TAIR, String.valueOf(namespace));
            EagleEye.commitRpcContext(rpcContext);
		}
	}
    */
}
