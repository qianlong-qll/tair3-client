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
public class PrefixGet extends TestBase {
	@Test
	public void simpleGet() {
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
	public void simpleGetWithIllegalParameter() {
		
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
	public void simpleGetWithDataNotExist() {
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
	public void simpleGetWithDataExpired() {
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
	public void simpleGetWithCounter() {
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
