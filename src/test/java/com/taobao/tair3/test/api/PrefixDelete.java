package com.taobao.tair3.test.api;
import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairConstant;


public class PrefixDelete extends TestBase {
	@Test
	public void simpleDelete() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		byte[] value = UUID.randomUUID().toString().getBytes();
		try {
			TairOption opt = new TairOption(500, (short)0, 0);
			Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.OK, r.getCode());
			 
			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, null);
			assertEquals(ResultCode.OK, g.getCode());
			assertEquals(new ByteArray(value), new ByteArray((byte[])g.getResult()));
			assertEquals(1, g.getVersion());
			//assertEquals(0, g.getFlag());
			
			Result<Void> d = tair.prefixInvalidByProxy(ns, pkey, skey, opt);
			assertEquals(true, ResultCode.OK.equals(d.getCode()) || ResultCode.NOTEXISTS.equals(d.getCode()));
			
			
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
	public void simpleDeleteWithNotExist() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		byte[] value = UUID.randomUUID().toString().getBytes();
		try {
			//Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
			//assertEquals(ResultCode.OK, r.getCode());
			 
			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, null);
			assertEquals(ResultCode.NOTEXISTS, g.getCode());
		//	assertEquals(new ByteArray(value), new ByteArray(g.getResult()));
		//	assertEquals(1, g.getVersion());
			//assertEquals(0, g.getFlag());
			
			Result<Void> d = tair.prefixInvalidByProxy(ns, pkey, skey, opt);
			assertEquals(true, ResultCode.OK.equals(d.getCode()) || ResultCode.NOTEXISTS.equals(d.getCode()));
			
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
	public void simpleDeleteWithExpired() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		byte[] value = UUID.randomUUID().toString().getBytes();
		try {
			TairOption opt = new TairOption(500, (short)0, 2);
			Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.OK, r.getCode());
			 
			Thread.sleep(3000);
			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, null);
			assertEquals(ResultCode.NOTEXISTS, g.getCode());
			//assertEquals(new ByteArray(value), new ByteArray(g.getResult()));
			//assertEquals(1, g.getVersion());
			//assertEquals(0, g.getFlag());
			
			Result<Void> d = tair.prefixInvalidByProxy(ns, pkey, skey, opt);
			assertEquals(true, ResultCode.OK.equals(d.getCode()) || ResultCode.NOTEXISTS.equals(d.getCode()));
			
			
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
	public void simpleDeleteWithIllegalParameter() {
		try {
			byte[] key = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.put(ns, key, val, null);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<Void> d = tair.invalidByProxy(ns, null, null);
			
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
	}
}
