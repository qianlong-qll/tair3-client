package com.taobao.tair3.test.api;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.TairClient.Pair;
import com.taobao.tair3.client.TairClient.RequestOption;
import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairConstant;
public class Put extends TestBase {
	public TairOption opt = new TairOption(50000000, (short)0, 0);
	@Test
	public void simplePut() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		try {
			ns = 120;
			Result<Void> r = tair.put(ns, key, val, opt);
			assertEquals(ResultCode.OK, r.getCode());
			 
			Result<byte[]> g = tair.get(ns, key, null);
			assertEquals(ResultCode.OK, g.getCode());
			
			assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
			assertEquals(new ByteArray(val), new ByteArray(g.getResult()));
			assertEquals(1, g.getVersion());
			//assertEquals(0, g.getFlag());
			key = "PENGJIAN".getBytes(); 
			String s1 = "00001SUBKEY";
			String s2 = "00002SUBKEY";
			String s3 = "00003SUBKYE";
			Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
			kvs.put(s1.getBytes(), new Pair<byte[], RequestOption>(s1.getBytes(), new RequestOption()));
			kvs.put(s2.getBytes(), new Pair<byte[], RequestOption>(s2.getBytes(), new RequestOption()));
			kvs.put(s3.getBytes(), new Pair<byte[], RequestOption>(s3.getBytes(), new RequestOption()));
			ResultMap<byte[], Result<Void>> r1 = tair.prefixPutMulti(ns, key, kvs, null);
			assertEquals(ResultCode.OK, r1.getCode());
			List<byte[]> skeys = new ArrayList<byte[]>();
			skeys.add(s1.getBytes());
			skeys.add(s2.getBytes());
			skeys.add(s3.getBytes());
			
		//	ResultMap<byte[], Result<byte[]>> rg = tair.prefixGetMulti(ns, key, skeys, null);
		//	assertEquals(ResultCode.OK, rg.getCode());
			Result<List<Pair<byte[], Result<byte[]>>>> r2 = tair.getRange(ns, key, null, null, 0, 10, false, null);
			assertEquals(ResultCode.OK, r2.getCode());
		} catch (TairRpcError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairTimeout e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void simplePutWithIllegalParameter() {
		
		try {
			byte[] key = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.put(ns, null, val, opt);
			
		} catch (TairRpcError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairTimeout e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			byte[] key = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.put(ns, key, null, opt);
			
		} catch (TairRpcError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairTimeout e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.VALUE_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			byte[] key = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.put(ns, null, null, null);
			
		} catch (TairRpcError e) {
		 
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairTimeout e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			byte[] key = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.put((short)-1, key, val, opt);
			
		} catch (TairRpcError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairTimeout e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			byte[] key = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.put((short)(TairConstant.NAMESPACE_MAX), key, val, null);
			
		} catch (TairRpcError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairTimeout e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
		}
	}
	
	@Test
	public void PutWithVersion() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		try {
			//TairOption opt = new TairOption(500, (short)0, 0);
			Result<Void> r = tair.put(ns, key, val, opt);
			assertEquals(ResultCode.OK, r.getCode());
			 
			Result<byte[]> g = tair.get(ns, key, null);
			assertEquals(ResultCode.OK, g.getCode());
			
			assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
			assertEquals(new ByteArray(val), new ByteArray(g.getResult()));
			assertEquals(1, g.getVersion());
			assertEquals(0, g.getFlag());
			
			
			opt.setVersion((short)4);
			Result<Void> r1 = tair.put(ns, key, val, opt);
			assertEquals(ResultCode.VERSION_ERROR, r1.getCode());
			
			opt.setVersion((short)1);
			Result<Void> r2 = tair.put(ns, key, val, opt);
			assertEquals(ResultCode.OK, r2.getCode());
			
			Result<byte[]> g1 = tair.get(ns, key, null);
			assertEquals(ResultCode.OK, g1.getCode());
			
			assertEquals(new ByteArray(key), new ByteArray(g1.getKey()));
			assertEquals(new ByteArray(val), new ByteArray(g1.getResult()));
			assertEquals(2, g1.getVersion());
			assertEquals(0, g1.getFlag());
			 
			
		} catch (TairRpcError e) {
			assertEquals(false, true);
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
			e.printStackTrace();
		} catch (TairTimeout e) {
			assertEquals(false, true);
			e.printStackTrace();
		} catch (InterruptedException e) {
			assertEquals(false, true);
			e.printStackTrace();
		}
	}
	
	@Test
	public void PutWithExipred() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		try {
			TairOption opt = new TairOption(500, (short) 0, 2);
			Result<Void> r = tair.put(ns, key, val, opt);
			assertEquals(ResultCode.OK, r.getCode());

			Thread.sleep(3000);
			 
			Result<byte[]> g = tair.get(ns, key, null);
			assertEquals(ResultCode.NOTEXISTS, g.getCode());
			 
		} catch (TairRpcError e) {
			assertEquals(false, true);
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
			e.printStackTrace();
		} catch (TairTimeout e) {
			assertEquals(false, true);
			e.printStackTrace();
		} catch (InterruptedException e) {
			assertEquals(false, true);
			e.printStackTrace();
		}

	}
}
