package com.taobao.tair3.client.rpc.protocol.protobuf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.taobao.tair3.client.rpc.protocol.protobuf.Echo.EchoRequest;
import com.taobao.tair3.client.rpc.protocol.protobuf.Echo.EchoResponse;

public class MessageManager {
	
	private Comparator<byte[]> messageKeyComp = new Comparator<byte[]>() {
		public int compare(byte[] o1, byte[] o2) {
			return ByteBuffer.wrap(o1).compareTo(ByteBuffer.wrap(o2));
		}
	};
	private Map<byte[], Builder> msgMap 
				= new TreeMap<byte[], Builder>(messageKeyComp);
	private Map<Class<? extends Message>, byte[]> clsMap 
				= new HashMap<Class<? extends Message>, byte[]>();
	
	public MessageManager() throws SecurityException, IllegalArgumentException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		//TODO: classloader found all class exntends message
		//TODO: just regist response class
		//TODO: short name or hashcode
		//regist(EchoResponse.newBuilder().build());
		regist(EchoResponse.class, EchoResponse.newBuilder());
		regist(EchoRequest.class, EchoRequest.newBuilder());
	}
	
	public byte[] getMessageType(Class< ? extends Message> mm) {
		return clsMap.get(mm);
	}
	
	public Builder getMessageClass(byte[] type) {
		return msgMap.get(type);
	}
	
	public void regist(Class< ? extends Message> mm, Builder builder) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		Method method = mm.getMethod("getDescriptor");
		Descriptor desp = (Descriptor)method.invoke(mm);
		byte[] typename = desp.getFullName().getBytes();
		if (msgMap.get(typename) != null || clsMap.get(mm) != null) {
			throw new IllegalArgumentException("bug: " + mm.getName() + " has exists");
		}
		
		msgMap.put(typename, builder);
		clsMap.put(mm, typename);
	}
	
	public static void main(String[] args) {
		
	}
}
