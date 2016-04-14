package com.taobao.tair3.client.impl;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.invalid.InvalidServer;
import com.taobao.tair3.client.packets.AbstractPacket;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.packets.AbstractResponsePacket;
import com.taobao.tair3.client.rpc.future.TairResultFutureImpl;
import com.taobao.tair3.client.rpc.net.TairBgWorker;
import com.taobao.tair3.client.rpc.net.TairRpcContext;
import com.taobao.tair3.client.rpc.net.TairRpcPacketFactory;
import com.taobao.tair3.client.rpc.protocol.tair2_3.PacketFactory;

public class TairProcessor {
	//private static Logger logger = LoggerFactory.getLogger(TairProcessor.class);
	private TairRpcContext context = null;
	
	protected ServerManager serverManager;
	private TairRpcPacketFactory packet2_3Factory = PacketFactory.getInstance();
	
	private static ConfigServerUpdater csUpdater		  = new ConfigServerUpdater();
	private static TairBgWorker		   timeoutCheckWorker = new TairBgWorker();
	private String group = null;
	static {
		timeoutCheckWorker.start();
		csUpdater.start();
	}
	
	public static void shutdown() {
		timeoutCheckWorker.interrupt();
		csUpdater.shutdown();
	}
	public TairProcessor(String master, String slave, String group, NioClientSocketChannelFactory nioFactory) {
		this.group = group;
		context = new TairRpcContext(nioFactory, timeoutCheckWorker, this.group);
		serverManager = new ServerManager(master, slave, group, context, csUpdater);
		context.setServerManager(serverManager);
		
	}
	
	public void init() throws TairException {
		serverManager.init();
	}
	 	
	public ServerManager getServerManager() {
		return serverManager;
	}

	public SocketAddress matchDataServer(byte[] prefix, byte[] key) throws TairRpcError {
		SocketAddress addr = serverManager.findDataServer(prefix, key);
		if (addr == null) {
			serverManager.maybeForceCheckVersion();
			throw new TairRpcError("not find any DataServer");
		}
		return addr;
	}
	public SocketAddress matchDataServer(byte[] key) throws TairRpcError {
		return matchDataServer(null, key);
	}
	 
	public Map<SocketAddress, List<byte[]>> matchDataServer(byte[] prefix, List<byte[]> keys) throws TairRpcError{
		HashMap<SocketAddress, List<byte[]>> result = new HashMap<SocketAddress, List<byte[]>>();
		for (byte[] key : keys) {
			SocketAddress addr = serverManager.findDataServer(prefix, key);
			if (addr == null) {
				throw new TairRpcError("not find any DataServer");
			}
			List<byte[]> keyList = result.get(addr);
			if (keyList == null) {
				keyList = new ArrayList<byte[]> ();
				keyList.add(key);
				result.put(addr, keyList);
			} else {
				keyList.add(key);
			}
		}
		return result;	
	}
	public Map<SocketAddress, List<byte[]>> matchDataServer(List<byte[]> keys) throws TairRpcError{
		return matchDataServer(null, keys);
	}
		
	public <S extends AbstractResponsePacket, T> TairResultFutureImpl<S, Result<T>> callDataServerAsync(SocketAddress addr, AbstractRequestPacket req, long timeout, Class<S> respCls, TairResultCast<S, Result<T>> cast) 
			throws TairRpcError, TairFlowLimit {
		if (addr == null) {
			throw new TairRpcError("Message: not find any DataServer, GroupName: " + this.group +", AREA: " + req.getNamespace());
		}
		return context.callAsync(addr, req, timeout, respCls, packet2_3Factory, cast);
	}

	public <S extends AbstractResponsePacket, T> TairResultFutureImpl<S, Result<T>> callInvalidServerAsync(AbstractRequestPacket req, long timeout, Class<S> respCls, TairResultCast<S, Result<T>> cast) 
			throws TairRpcError, TairFlowLimit {
		InvalidServer invalServer = serverManager.chooseInvalidServer();
		if (invalServer == null) {
			//throw new TairRpcError("not find any InvalServer.");
			return null;
		}
		return context.callInvalServerAsync(invalServer, req, timeout, respCls, packet2_3Factory, cast);
		 
	}
	
	public <S extends AbstractResponsePacket, T> TairResultFutureImpl<S, Result<T>> callConfigServerAsync(SocketAddress addr, AbstractRequestPacket req, long timeout, Class<S> respCls, TairResultCast<S, Result<T>> cast) 
			throws TairRpcError, TairFlowLimit {
		if (addr == null) {
			throw new TairRpcError("ConfigServer's address is null, GroupName: " + this.group);
		}
		return context.callAsync(addr, req, timeout, respCls, packet2_3Factory, cast);
	}

	public interface TairResultCast<S, T> {
		public T cast(S s, Object context) throws TairRpcError, TairCastIllegalContext;
	}
	 
}
