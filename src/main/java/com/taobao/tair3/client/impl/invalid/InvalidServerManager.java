package com.taobao.tair3.client.impl.invalid;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.taobao.tair3.client.rpc.net.TairRpcContext;
import com.taobao.tair3.client.util.TairUtil;

public class InvalidServerManager {
	protected static final Logger log 	= LoggerFactory.getLogger(InvalidServerManager.class);
    private InvalidServer[] servers 	= new InvalidServer[0];
    private AtomicInteger lastSeq 		= new AtomicInteger((int)Math.random() * 10);
    
    protected TairRpcContext context;
    protected long pingTimeout = 5000;
    protected Set<SocketAddress> aliveNodes = null;
    private static final String INVALID_KEY = "invalidate_server";
    
    public void setPingTimeout(long t) {
    	this.pingTimeout = t;
    }
  
    public InvalidServerManager(TairRpcContext context) {
    	this.context = context;
    }

	public void update(Map<String, String> config, Set<SocketAddress> aliveNodes) {

		String confs = config.get(INVALID_KEY);
		if (confs == null) {
			servers = new InvalidServer[0];
			log.warn("no invalid server found");
			return ;
		}

		Set<InvalidServer> iplist = new HashSet<InvalidServer>();
		for (String address : confs.split(",")) {
			if (address == null || "".equals(address.trim())) {
				log.error("the address of invalserver is not available");
				continue;
			}
			log.info("got invalid server " + address);
			try {
				iplist.add(new InvalidServer(TairUtil.cast2SocketAddress(address)));
			} catch (IllegalArgumentException e) {
				log.error("init invalid server execption: ", e);
			}

		}
		log.debug("update the invalserver list.");
		servers = iplist.toArray(new InvalidServer[0]);
		this.aliveNodes = aliveNodes;
	}

	public InvalidServer findInvalidServer() {
		for (int i = 0; i < servers.length; ++i) {
			int seq = Math.abs(lastSeq.incrementAndGet()) % servers.length;
			InvalidServer server = servers[seq];
//			if (aliveNodes.contains(server.getAddress())) {
				return server;
//			}
		}
		return null;
	}
}
