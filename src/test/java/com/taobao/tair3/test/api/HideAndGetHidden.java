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
public class HideAndGetHidden extends TestBase {
	@Test
	public void simpleHide() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		try {
			Result<Void> rd = tair.invalidByProxy(ns, key, null);
			assertEquals(true , rd.getCode().equals(ResultCode.OK) || rd.getCode().equals(ResultCode.NOTEXISTS));
			
			Result<Void> rp = tair.put(ns, key, val, null);
			assertEquals(ResultCode.OK, rp.getCode());
			
			Result<byte[]> rg = tair.get(ns, key, null);
			assertEquals(ResultCode.OK, rg.getCode());
			assertEquals(new ByteArray(key), new ByteArray(rg.getKey()));
			assertEquals(new ByteArray(val), new ByteArray(rg.getResult()));
			
			Result<Void>  rh = tair.hideByProxy(ns, key, null); 
			assertEquals(ResultCode.OK, rh.getCode());
			
			Result<byte[]> rg1 = tair.get(ns, key, null);
			assertEquals(ResultCode.HIDDEN, rg1.getCode());
			
			Result<byte[]> rgh = tair.getHidden(ns, key, null);
			assertEquals(ResultCode.OK, rgh.getCode());
			assertEquals(new ByteArray(key), new ByteArray(rgh.getKey()));
			assertEquals(new ByteArray(val), new ByteArray(rgh.getResult()));
			
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
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		try {
			int initValue = 0;
			int value = 1;
			Result<Void> rd = tair.invalidByProxy(ns, key, null);
			assertEquals(true , rd.getCode().equals(ResultCode.OK) || rd.getCode().equals(ResultCode.NOTEXISTS));
			
			Result<Void> rp = tair.setCount(ns, key, initValue, null);
			assertEquals(ResultCode.OK, rp.getCode());
			
			Result<byte[]> rg = tair.get(ns, key, null);
			assertEquals(ResultCode.OK, rg.getCode());
			 
			 
			
			Result<Void>  rh = tair.hideByProxy(ns, key, null); 
			assertEquals(ResultCode.OK, rh.getCode());
			
			Result<byte[]> rg1 = tair.get(ns, key, null);
			assertEquals(ResultCode.HIDDEN, rg1.getCode());
			
			Result<byte[]> rgh = tair.getHidden(ns, key, null);
			assertEquals(ResultCode.OK, rgh.getCode());
			assertEquals(new ByteArray(key), new ByteArray(rgh.getKey()));
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
