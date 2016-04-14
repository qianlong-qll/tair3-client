package com.taobao.tair3.client.rpc.protocol.tair2_3;

import java.util.HashMap;

import com.taobao.tair3.client.packets.common.BatchReturnResponse;
import com.taobao.tair3.client.packets.common.PingRequest;
import com.taobao.tair3.client.packets.common.ReturnResponse;
import com.taobao.tair3.client.packets.configserver.GetGroupRequest;
import com.taobao.tair3.client.packets.configserver.GetGroupResponse;
import com.taobao.tair3.client.packets.configserver.QueryInfoRequest;
import com.taobao.tair3.client.packets.configserver.QueryInfoResponse;
import com.taobao.tair3.client.packets.dataserver.BatchPutRequest;
import com.taobao.tair3.client.packets.dataserver.BoundedIncDecRequest;
import com.taobao.tair3.client.packets.dataserver.BoundedIncDecResponse;
import com.taobao.tair3.client.packets.dataserver.BoundedPrefixIncDecRequest;
import com.taobao.tair3.client.packets.dataserver.BoundedPrefixIncDecResponse;
import com.taobao.tair3.client.packets.dataserver.DeleteRequest;
import com.taobao.tair3.client.packets.dataserver.ExpireRequest;
import com.taobao.tair3.client.packets.dataserver.GetHiddenRequest;
import com.taobao.tair3.client.packets.dataserver.GetRequest;
import com.taobao.tair3.client.packets.dataserver.GetResponse;
import com.taobao.tair3.client.packets.dataserver.HideRequest;
import com.taobao.tair3.client.packets.dataserver.IncDecRequest;
import com.taobao.tair3.client.packets.dataserver.IncDecResponse;
import com.taobao.tair3.client.packets.dataserver.LockRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixDeleteMultiRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixIncDecRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixIncDecResponse;
import com.taobao.tair3.client.packets.dataserver.RangeRequest;
import com.taobao.tair3.client.packets.dataserver.RangeResponse;
import com.taobao.tair3.client.packets.dataserver.SimplePrefixGetMultiRequest;
import com.taobao.tair3.client.packets.dataserver.SimplePrefixGetMultiResponse;
import com.taobao.tair3.client.packets.dataserver.TrafficCheckRequest;
import com.taobao.tair3.client.packets.dataserver.TrafficCheckResponse;

import com.taobao.tair3.client.packets.dataserver.PrefixGetHiddenMultiRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixGetMultiRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixGetMultiResponse;
import com.taobao.tair3.client.packets.dataserver.PrefixHideMultiRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixPutMultiRequest;
import com.taobao.tair3.client.packets.dataserver.PutRequest;
import com.taobao.tair3.client.packets.invalidserver.HideByProxyMultiRequest;
import com.taobao.tair3.client.packets.invalidserver.HideByProxyRequest;
import com.taobao.tair3.client.packets.invalidserver.InvalidByProxyMultiRequest;
import com.taobao.tair3.client.packets.invalidserver.InvalidByProxyRequest;

public class PacketManager {
	
	private static final int REQ_PUT    = 1;
	private static final int REQ_MPUT = 2129;
	private static final int REQ_GET    = 2;

	private static final int REQ_DELETE = 3;
	private static final int REQ_INCDEC = 11;
	private static final int RESP_INCDEC = 105;
	private static final int REQ_LOCK = 14;
	private static final int REQ_EXPIRE = 2127;
	
 
	private static final int REQ_PING 	= 6;
	
	private static final int REQ_RANGE = 18;
	private static final int RESP_RANGE = 19;
	private static final int REQ_HIDE = 20;
	private static final int REQ_HIDE_PROXY = 21;
	private static final int REQ_GET_HIDDEN = 22;
	private static final int REQ_INVAL_PROXY = 23;
	private static final int REQ_INVAL_PREFIX_HIDE_MULTI_BY_PROXY = 33;
	private static final int REQ_INVAL_PREFIX_INVALID_MULTI_BY_PROXY = 32;
	
	
	private static final int REQ_PREFIX_PUTS = 24;
	private static final int REQ_PREFIX_GETS = 29;
	private static final int RESP_PREFIX_GETS = 30;
	private static final int REQ_PREFIX_DELETES = 25;
	private static final int REQ_PREFIX_HIDES = 31;
	private static final int REQ_PREFIX_GET_HIDDENS = 34;
	
	private static final int RESP_GET   = 102;
	private static final int RET_PCK 	= 101;
	private static final int RESP_BATCH_RETURN = 28;

	private static final int REQ_GET_GROUP = 1002;
	private static final int RESP_GET_GROUP = 1102;
	private static final int REQ_PREFIX_INCDEC = 26;
	private static final int RESP_PREFIX_INCDEC = 27;
	private static final int REQ_BOUNDED_INCDEC = 1704;
	private static final int RESP_BOUNDED_INCDEC = 1705;
	private static final int REQ_BOUNDED_PREFIX_INCDEC = 1706;
	private static final int RESP_BOUNDED_PREFIX_INCDEC = 1707;
	
	private static final int TAIR_REQ_QUERY_INFO = 1009;
	private static final int TAIR_RESP_QUERY_INFO = 1106;
	private static final int TAIR_REQ_SIMPLE_GET = 36;
	private static final int TAIR_RESP_SIMPLE_GET = 37;
	
	private static final int TAIR_FLOW_CONTROL = 9001;
	private static final int TAIR_FLOW_CHECK = 9005;

	private static HashMap<Integer, Class<? extends Packet>> codePacketMap 
		= new HashMap<Integer, Class<? extends Packet>>();
	
	private static HashMap<Class<? extends Packet>, Integer> packetCodeMap 
		= new HashMap<Class<? extends Packet>, Integer>();
	
	static {
		PacketManager.regist(REQ_GET, GetRequest.class);
		PacketManager.regist(RESP_GET, GetResponse.class);
		PacketManager.regist(REQ_PUT, PutRequest.class);
		PacketManager.regist(RET_PCK, ReturnResponse.class);
		PacketManager.regist(REQ_GET_GROUP, GetGroupRequest.class);
		PacketManager.regist(RESP_GET_GROUP, GetGroupResponse.class);
		PacketManager.regist(REQ_INVAL_PROXY, InvalidByProxyRequest.class);
		PacketManager.regist(REQ_HIDE_PROXY, HideByProxyRequest.class);
		PacketManager.regist(REQ_INVAL_PREFIX_HIDE_MULTI_BY_PROXY, HideByProxyMultiRequest.class);
		PacketManager.regist(REQ_INVAL_PREFIX_INVALID_MULTI_BY_PROXY, InvalidByProxyMultiRequest.class);
		PacketManager.regist(REQ_PING, PingRequest.class);
		PacketManager.regist(REQ_MPUT, BatchPutRequest.class);
		PacketManager.regist(REQ_EXPIRE, ExpireRequest.class);
		PacketManager.regist(REQ_DELETE, DeleteRequest.class);
		PacketManager.regist(REQ_INCDEC, IncDecRequest.class);
		PacketManager.regist(RESP_INCDEC, IncDecResponse.class);
		PacketManager.regist(REQ_LOCK, LockRequest.class);
		PacketManager.regist(REQ_HIDE, HideRequest.class);
		PacketManager.regist(REQ_GET_HIDDEN, GetHiddenRequest.class);
		PacketManager.regist(REQ_PREFIX_PUTS, PrefixPutMultiRequest.class);
		PacketManager.regist(REQ_PREFIX_GETS, PrefixGetMultiRequest.class);
		PacketManager.regist(RESP_PREFIX_GETS, PrefixGetMultiResponse.class);
		PacketManager.regist(REQ_PREFIX_DELETES, PrefixDeleteMultiRequest.class);
		PacketManager.regist(RESP_BATCH_RETURN, BatchReturnResponse.class);
		PacketManager.regist(REQ_PREFIX_HIDES, PrefixHideMultiRequest.class);
		PacketManager.regist(REQ_PREFIX_GET_HIDDENS, PrefixGetHiddenMultiRequest.class);
		PacketManager.regist(REQ_PREFIX_INCDEC, PrefixIncDecRequest.class);
		PacketManager.regist(RESP_PREFIX_INCDEC, PrefixIncDecResponse.class);
		PacketManager.regist(REQ_RANGE, RangeRequest.class);
		PacketManager.regist(RESP_RANGE, RangeResponse.class);
		PacketManager.regist(REQ_BOUNDED_INCDEC, BoundedIncDecRequest.class);
		PacketManager.regist(RESP_BOUNDED_INCDEC, BoundedIncDecResponse.class);
		PacketManager.regist(REQ_BOUNDED_PREFIX_INCDEC, BoundedPrefixIncDecRequest.class);
		PacketManager.regist(RESP_BOUNDED_PREFIX_INCDEC, BoundedPrefixIncDecResponse.class);
		PacketManager.regist(TAIR_REQ_QUERY_INFO, QueryInfoRequest.class);
		PacketManager.regist(TAIR_RESP_QUERY_INFO, QueryInfoResponse.class);
		PacketManager.regist(TAIR_REQ_SIMPLE_GET, SimplePrefixGetMultiRequest.class);
		PacketManager.regist(TAIR_RESP_SIMPLE_GET, SimplePrefixGetMultiResponse.class);
		PacketManager.regist(TAIR_FLOW_CONTROL, TrafficCheckResponse.class);
		PacketManager.regist(TAIR_FLOW_CHECK, TrafficCheckRequest.class);
	}
	
	public static void regist(Integer packetCode, Class<? extends Packet> cls) {
		if (codePacketMap.containsKey(packetCode) || packetCodeMap.containsKey(cls))
			throw new IllegalArgumentException("Packet Code " + packetCode + " already exists");
		codePacketMap.put(packetCode, cls);
		packetCodeMap.put(cls, packetCode);
	}
	
	public static Integer getPacketCode(Class<? extends Packet> cls) {
		return packetCodeMap.get(cls);
	}
	
	public static Class<? extends Packet> getPacketClass(Integer code) {
		return codePacketMap.get(code);
	}
}
