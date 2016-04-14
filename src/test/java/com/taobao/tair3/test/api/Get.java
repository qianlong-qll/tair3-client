package com.taobao.tair3.test.api;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairConstant;
public class Get extends TestBase {
	 
	public void simplePrefixPut() {
		String _pkey = UUID.randomUUID().toString();
		String _skey = UUID.randomUUID().toString();
		String _val = UUID.randomUUID().toString();
		byte[] pkey = _pkey.getBytes();
		byte[] skey = _skey.getBytes();
		byte[] val = _val.getBytes();
		 
		ns = 120;
		for (int i =0; i < 2;) {
		try { 
			
			Result<Void> r = tair.prefixPut(ns, pkey, skey, val, opt);
			assertEquals(ResultCode.OK, r.getCode());
			 
			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, null);
			assertEquals(ResultCode.OK, g.getCode());
			
			System.out.print(".");
			//assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
		//	assertEquals(new ByteArray(val), new ByteArray(g.getResult()));
			//assertEquals(1, g.getVersion());
			//assertEquals(0, g.getFlag());
			 
		} catch (TairRpcError e) {
			e.printStackTrace();
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			e.printStackTrace();
			assertEquals(false, true);
		} catch (TairTimeout e) {
			e.printStackTrace();
			assertEquals(false, true);
		} catch (InterruptedException e) {
			e.printStackTrace();
			assertEquals(false, true);
		}
		}
	}

	@Test
	public void simpleGet() {
		ns = 120;
		TairOption opt = new TairOption(500000);
		for (int i = 0; i < 10; ++i) {
			byte[] key = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			try {
				Result<Void> r = tair.put(ns, key, val, opt);
				assertEquals(ResultCode.OK, r.getCode());

				Result<byte[]> g = tair.get(ns, key, opt);
				assertEquals(ResultCode.OK, g.getCode());

				//assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
				//assertEquals(new ByteArray(val), new ByteArray(g.getResult()));
				//assertEquals(1, g.getVersion());
				// assertEquals(0, g.getFlag());

				//System.out.println("DONE = " + i);
			} catch (TairRpcError e) {
				//assertEquals(false, true);
				e.printStackTrace();
			} catch (TairFlowLimit e) {
				//assertEquals(false, true);
				e.printStackTrace();
			} catch (TairTimeout e) {
				//assertEquals(false, true);
				e.printStackTrace();
			} catch (InterruptedException e) {
				//assertEquals(false, true);
				e.printStackTrace();
			}
		}
	}
	
	//@Test
	public void simpleGetWithIllegalParameter() {
		
		try {
			byte[] key = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.put(ns, key, val, null);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.get(ns, null, null);
			
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
			Result<Void> r = tair.put(ns, key, val, null);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.get((short)-1, key, null);
			
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
			Result<Void> r = tair.put(ns, key, val, null);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.get((short)TairConstant.NAMESPACE_MAX, key, null);
			
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
	
	//@Test
	public void simpleGetWithDataNotExist() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		try {
			Result<Void> r = tair.invalidByProxy(ns, key, null);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.get(ns, key, null);
			assertEquals(ResultCode.NOTEXISTS, g.getCode());
			
		} catch (TairRpcError e) {
			//assertEquals(false, true);
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
		
	//@Test
	public void simpleGetWithDataExpired() {
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
	
	@Test
	public void simpleLockUnlock() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		try {
			Result<Void> r = tair.put(ns, key, val, null);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.get(ns, key, null);
			assertEquals(ResultCode.OK, g.getCode());
			
			assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
			assertEquals(new ByteArray(val), new ByteArray(g.getResult()));
			assertEquals(1, g.getVersion());
			//assertEquals(0, g.getFlag());
			
			Result<Void> l = tair.lock(ns, key, null);
			assertEquals(ResultCode.OK, l.getCode());
			
			Result<Void> u = tair.unlock(ns, key, null);
			assertEquals(ResultCode.OK, u.getCode());
			
		} catch (TairRpcError e) {
			e.printStackTrace();
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			e.printStackTrace();
			assertEquals(false, true);
		} catch (TairTimeout e) {
			e.printStackTrace();
			assertEquals(false, true);
		} catch (InterruptedException e) {
			e.printStackTrace();
			assertEquals(false, true);
		}
	}
	
	//@Test
	public void simpleGetWithCounter() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		int value = 11;
		int defaultValue = 0;
		try {
			Result<Integer> r = tair.incr(ns, key, value, defaultValue, null);
			assertEquals(ResultCode.OK, r.getCode());

			TairOption opt = new TairOption(50000, (short)0, 0);
			Result<byte[]> g = tair.get(ns, key, opt);
			assertEquals(ResultCode.OK, g.getCode());
			assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
			//assertEquals(true, g.isCounter());

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
	public void simpleGet1() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		byte[] value = UUID.randomUUID().toString().getBytes();
		try {
			Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, opt);
			assertEquals(ResultCode.OK, g.getCode());
			
			//assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
			assertEquals(new ByteArray(value), new ByteArray(g.getResult()));
			assertEquals(1, g.getVersion());
			//assertEquals(0, g.getFlag());
			
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
	public void simpleGetWithIllegalParameter1() {
		
		try {
			byte[] pkey = UUID.randomUUID().toString().getBytes();
			byte[] skey = UUID.randomUUID().toString().getBytes();
			byte[] value = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.prefixGet(ns, null, skey, opt);
			assertEquals(ResultCode.OK, g.getCode());
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
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			byte[] pkey = UUID.randomUUID().toString().getBytes();
			byte[] skey = UUID.randomUUID().toString().getBytes();
			byte[] value = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.prefixGet(ns, pkey, null, opt);
			assertEquals(ResultCode.OK, g.getCode());
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
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			byte[] pkey = UUID.randomUUID().toString().getBytes();
			byte[] skey = UUID.randomUUID().toString().getBytes();
			byte[] value = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.prefixGet((short)-1, pkey, skey, opt);
			assertEquals(ResultCode.OK, g.getCode());
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
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
		}
	}
	
	@Test
	public void simpleGetWithDataNotExist1() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		byte[] value = UUID.randomUUID().toString().getBytes();
		try {
			Result<Void> r = tair.prefixInvalidByProxy(ns, pkey, skey, null);
			assertEquals(true, r.getCode().equals(ResultCode.NOTEXISTS) || r.getCode().equals(ResultCode.OK));
			
			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, null);
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
		
	@Test
	public void simpleGetWithDataExpired1() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		byte[] value = UUID.randomUUID().toString().getBytes();
		try {
			TairOption opt = new TairOption(500, (short) 0, 2);
			Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.OK, r.getCode());

			Thread.sleep(3000);

			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, opt);
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
	
	@Test
	public void simpleGetWithCounter1() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		int value = 11;
		int defaultValue = 0;
		try {
			Result<Integer> r = tair.prefixIncr(ns, pkey, skey, value, defaultValue, opt);
			assertEquals(ResultCode.OK, r.getCode());

			TairOption opt = new TairOption(50000, (short)0, 0);
			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, opt);
			assertEquals(ResultCode.OK, g.getCode());

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
