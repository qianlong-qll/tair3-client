/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair3.client.impl;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.taobao.eagleeye.EagleEye;
//import com.taobao.eagleeye.RpcContext_inner;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.invalid.InvalidServer;
import com.taobao.tair3.client.impl.invalid.InvalidServerManager;
import com.taobao.tair3.client.packets.configserver.GetGroupRequest;
import com.taobao.tair3.client.packets.configserver.GetGroupResponse;
import com.taobao.tair3.client.rpc.future.TairResultFutureImpl;
import com.taobao.tair3.client.rpc.net.TairRpcContext;
import com.taobao.tair3.client.rpc.protocol.tair2_3.PacketFactory;
import com.taobao.tair3.client.rpc.protocol.tair2_3.PacketManager;
import com.taobao.tair3.client.util.TairUtil;

public class ServerManager {
	protected static final Logger log = LoggerFactory.getLogger(ServerManager.class);
	
	private final int UPDATER_INTERVAL 	= 2000; // 2s
	private final int UPDATER_TIMEOUT 	= 2000; // 2s
	
	private String groupName = null;
	private String rowGroupName = null;
	private int configServerVersion = 0;
	
	private long 	lastUpdateTime = 0;
	private int 	nextConfigServerIndex = 0;
	private List<SocketAddress> csList = new ArrayList<SocketAddress>();
	private List<SocketAddress> dsList = new ArrayList<SocketAddress>();
	private Set<SocketAddress> aliveSet = new HashSet<SocketAddress>();
 
	
	private InvalidServerManager invalidServerManager = null;

	private int bucketCount = 0;
	private int copyCount = 0;
	
	private TairRpcContext context;
	
	private ConfigServerUpdater updater;

	private AtomicInteger failedCounter = new AtomicInteger(0);
	private int maxFailedCount = 100;
	public ServerManager(String master, String slave, String group, TairRpcContext context, ConfigServerUpdater updater) {
		if (master == null || group == null) 
			throw new IllegalArgumentException();
		this.groupName = group;
		if (group.endsWith("\0")) {
			this.rowGroupName = group.substring(0, group.length() - 1);
		}
		else {
			this.rowGroupName = group;
		}
		csList.add(TairUtil.cast2SocketAddress(master.trim()));
		if (slave != null)
			csList.add(TairUtil.cast2SocketAddress(slave.trim()));
		this.context = context;
		invalidServerManager = new InvalidServerManager(context);
		this.updater = updater;
	}
	
	public void init() throws TairException {
		while (true) {
			TairResultFutureImpl<GetGroupResponse, Result<Void>> future = null;
			try {
				future = asyncGrabGroupConfig();
			} catch (Exception e) {
				log.warn("send request of group list faield ", e);
				continue;
			}
			if (future == null) 
				throw new TairException("no config server useable");
			try {
				GetGroupResponse response = future.getResponse();
				update(response);
				break;
			} catch (Exception e) {
				log.warn("init group list faield ", e);
				continue;
			}
		}
		
	}
	
	public void checkVersion(int version) {
		if (version == this.configServerVersion || version <= 0 
				|| this.lastUpdateTime + UPDATER_INTERVAL >= System.currentTimeMillis())
			return;
		else if (updater.submit(this))
			this.lastUpdateTime = System.currentTimeMillis();
	}
	public void maybeForceCheckVersion() {
		if (failedCounter.incrementAndGet() > maxFailedCount) {
			failedCounter.set(0);
			if (updater.submit(this)) {
				this.lastUpdateTime = System.currentTimeMillis();
			}
		}
	}
	
	public TairResultFutureImpl<GetGroupResponse, Result<Void>> asyncGrabGroupConfig() throws TairRpcError, TairFlowLimit {
		TairResultFutureImpl<GetGroupResponse, Result<Void>> future = null;
		if (nextConfigServerIndex >= csList.size()) {
			nextConfigServerIndex = 0;
		} else
			try {
					GetGroupRequest request = new GetGroupRequest(this.groupName, this.configServerVersion);
					future = context.callAsyncUnlimit(csList.get(nextConfigServerIndex), request, UPDATER_TIMEOUT, GetGroupResponse.class, PacketFactory.getInstance(), null);
					nextConfigServerIndex++;
			} catch (Exception e) {
				 
			}
		return future;
	}
	
	public void update(GetGroupResponse response) {
		int prevConfigSerVersion = this.configServerVersion;
		this.nextConfigServerIndex = 0;
		this.lastUpdateTime        = System.currentTimeMillis();
		if (response.getBucketCount() <= 0)
			 throw new IllegalArgumentException("bucket count count can not be 0, or can't find group " + groupName);
		if (response.getCopyCount() <= 0)
	          throw new IllegalArgumentException("bucket copy count can not be 0");
		if (response.getDataServers().size() % response.getBucketCount() != 0)
			throw new IllegalArgumentException("dataserver.size % bucketcount != 0");
		
		if (configServerVersion == response.getConfigVersion()) {
			log.info("same config version " + this.configServerVersion);
		} else {
			configServerVersion = response.getConfigVersion();
			bucketCount = response.getBucketCount();
			copyCount = response.getCopyCount();

			dsList = response.getDataServers();
			aliveSet = response.getAliveServers();

			invalidServerManager.update(response.getConfigs(), aliveSet);
			if (log.isInfoEnabled()) {
				log.info("update to " + configServerVersion + " ds:" + dsList + " alive:" + aliveSet);
			}
			log.warn("group: " + this.groupName + " configversion updated from : " + prevConfigSerVersion + " to: " + configServerVersion);
            /*
			// [EagleEye]
			{
				EagleEye.startRpc(String.valueOf(PacketManager.getPacketCode(GetGroupRequest.class)), getRowGroupName());
				RpcContext_inner rpcContext = EagleEye.popRpcContext();
				if (csList != null && csList.get(0) != null) {
					String master = csList.get(0).toString();
					rpcContext.setRemoteIp(master);
				}
				rpcContext.setCallBackMsg("cs");
				String msg = prevConfigSerVersion + ":" + configServerVersion;
				rpcContext.endRpc(msg, EagleEye.TYPE_TAIR, null);
				rpcContext.rpcClientSend();
				EagleEye.commitRpcContext(rpcContext);
			}
            */
		}
	}

	protected void resetConfigVersion() {
		configServerVersion = 0;
	}

	public int getCopyCount() {
		return copyCount;
	}

	protected int findServerIndex(byte[] prefix, byte[] index) {
		ChannelBuffer buffer = null;
		if (prefix != null) {
			buffer = ChannelBuffers.wrappedBuffer(prefix, index, null);
		}
		else {
			buffer = ChannelBuffers.wrappedBuffer(index, null);
		}
		long hash = TairUtil.murMurHash(buffer);
		if ((dsList != null) && (dsList.size() > 0))
			return (int) (hash %= bucketCount);
		else {
			return -1;
		}
	}

	public int getConfigVersion() {
		return configServerVersion;
	}

	public List<SocketAddress> getConfigServers() {
		return csList;
	}
	
	public String getGroupName() {
		return groupName;
	}
	
	public String getRowGroupName() {
		return rowGroupName;
	}

	public SocketAddress findDataServer(byte[] prefix, byte[] index) {
		return findDataServer(prefix, index, true);
	}
	
	public SocketAddress findDataServer(byte[] prefix, byte[] index, boolean check) {
		int idx = findServerIndex(prefix, index);
		if (idx == -1) {
			return null;
		}
		SocketAddress addr = dsList.get(idx);
		if (check && !aliveSet.contains(addr)) {
			return null;
		}
		return addr;
	}
	
	public InvalidServer chooseInvalidServer() {
		return invalidServerManager.findInvalidServer();
	}
	
}
