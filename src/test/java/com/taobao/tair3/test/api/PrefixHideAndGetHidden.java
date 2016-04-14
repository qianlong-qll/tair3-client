package com.taobao.tair3.test.api;
import static org.junit.Assert.*;

import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.ByteArray;
public class PrefixHideAndGetHidden extends TestBase {
	@Test
	public void simpleHide() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		byte[] value = UUID.randomUUID().toString().getBytes();
		try {
			Result<Void> rd = tair.prefixInvalidByProxy(ns, pkey, skey, null);
			assertEquals(true , rd.getCode().equals(ResultCode.OK) || rd.getCode().equals(ResultCode.NOTEXISTS));
			
			Result<Void> rp = tair.prefixPut(ns, pkey, skey, value, null);
			assertEquals(ResultCode.OK, rp.getCode());
			
			Result<byte[]> rg = tair.prefixGet(ns, pkey, skey, null);
			assertEquals(ResultCode.OK, rg.getCode());
			 
			
			Result<Void>  rh = tair.prefixHideByProxy(ns, pkey, skey, null);
			assertEquals(ResultCode.OK, rh.getCode());
			
			Result<byte[]> rg1 = tair.prefixGet(ns, pkey, skey, null);
			assertEquals(ResultCode.HIDDEN, rg1.getCode());
			
			Result<byte[]> rgh = tair.prefixGetHidden(ns, pkey, skey, null);
			assertEquals(ResultCode.OK, rgh.getCode());
			 
			
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
	public void simpleHideCounter() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		try {
			int initValue = 0;
			int value = 1;
			Result<Void> rd = tair.prefixInvalidByProxy(ns, pkey, skey, null);
			assertEquals(true , rd.getCode().equals(ResultCode.OK) || rd.getCode().equals(ResultCode.NOTEXISTS));
			
			Result<Void> rp = tair.prefixSetCount(ns, pkey, skey, initValue, null);
			assertEquals(ResultCode.OK, rp.getCode());
			
			Result<byte[]> rg = tair.prefixGet(ns, pkey, skey, null);
			assertEquals(ResultCode.OK, rg.getCode());
			 
			 
			
			Result<Void>  rh = tair.prefixHideByProxy(ns, pkey, skey, null);
			assertEquals(ResultCode.OK, rh.getCode());
			
			Result<byte[]> rg1 = tair.prefixGet(ns, pkey, skey, null);
			assertEquals(ResultCode.HIDDEN, rg1.getCode());
			
			Result<byte[]> rgh = tair.prefixGetHidden(ns, pkey, skey, null);
			assertEquals(ResultCode.OK, rgh.getCode());
			//assertEquals(new ByteArray(key), new ByteArray(rgh.getKey()));
			//assertEquals(new ByteArray(val), new ByteArray(rgh.getResult()));
			
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
