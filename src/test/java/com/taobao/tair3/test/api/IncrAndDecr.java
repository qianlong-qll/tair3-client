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
public class IncrAndDecr extends TestBase {
	@Test
	public void simpleIncrAndDecr() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		int value = 10;
		int value2 = 5;
		int defaultValue = 1;
		try {
			Result<Integer> i = tair.incr(ns, key, value, defaultValue, null);
			assertEquals(ResultCode.OK, i.getCode());
			assertEquals((value + defaultValue), i.getResult());
			assertEquals(0, i.getVersion());
			
			
			 
			Result<Integer> d = tair.decr(ns, key, value2, defaultValue, null);
			assertEquals(ResultCode.OK, i.getCode());
			assertEquals((value + defaultValue - value2), d.getResult());
			assertEquals(0, d.getVersion());
		} catch (TairRpcError e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (TairFlowLimit e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (TairTimeout e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (InterruptedException e) {
			e.printStackTrace();
			assertEquals(true, false);
		}
	}
	
	@Test
	public void simpleIncrAndDecrWithIllegalParameter() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		int value = 10;
		int value2 = 5;
		int defaultValue = 1;
		try {
			Result<Integer> i = tair.incr(ns, null, value, defaultValue, null);
			assertEquals(ResultCode.OK, i.getCode());
			assertEquals((value + defaultValue), i.getResult());
			assertEquals(0, i.getVersion());
			
			
			 
			Result<Integer> d = tair.decr(ns, key, value2, defaultValue, null);
			assertEquals(ResultCode.OK, d.getCode());
			assertEquals((value + defaultValue - value2), d.getResult());
			assertEquals(0, d.getVersion());
		} catch (TairRpcError e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (TairFlowLimit e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (TairTimeout e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (InterruptedException e) {
			e.printStackTrace();
			assertEquals(true, false);
		}
		catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
	}
	@Test
	public void simpleIncrAndDecrWithExpireTime() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		int value = 10;
		int value2 = 5;
		int defaultValue = 1;
		try {
			TairOption opt = new TairOption(500000, (short)0, 2);
			Result<Integer> i = tair.incr(ns, key, value, defaultValue, opt);
			assertEquals(ResultCode.OK, i.getCode());
			assertEquals((value + defaultValue), i.getResult());
			assertEquals(0, i.getVersion());
			
			Thread.sleep(3000);
			 
			Result<byte[]>  g = tair.get(ns, key, null);
			assertEquals(ResultCode.NOTEXISTS, g.getCode());
			
			Result<Integer> d = tair.decr(ns, key, value2, defaultValue, null);
			assertEquals(ResultCode.OK, d.getCode());
			assertEquals(0, d.getVersion());
		} catch (TairRpcError e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (TairFlowLimit e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (TairTimeout e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (InterruptedException e) {
			e.printStackTrace();
			assertEquals(true, false);
		}
		catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
	}
	
	@Test
	public void simpleIncrAndDecrWithExistKey() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		int value = 10;
		int value2 = 5;
		int defaultValue = 1;
		try {
			Result<Void> p = tair.put(ns, key, val, null);
			assertEquals(ResultCode.OK, p.getCode());
			
			 
			Result<Integer> i = tair.incr(ns, key, value, defaultValue, null);
			assertEquals(ResultCode.CANNOT_OVERRIDE, i.getCode());
			
			 
			 
			Result<byte[]>  g = tair.get(ns, key, null);
			assertEquals(ResultCode.OK, g.getCode());
			
			Result<Integer> d = tair.decr(ns, key, value2, defaultValue, null);
			assertEquals(ResultCode.CANNOT_OVERRIDE, d.getCode());
		} catch (TairRpcError e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (TairFlowLimit e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (TairTimeout e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (InterruptedException e) {
			e.printStackTrace();
			assertEquals(true, false);
		}
	}
	
	@Test
	public void simpleIncrAndDecrWithSetCounter() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		int value = 10;
		int value2 = 5;
		int defaultValue = 1;
		try {
			Result<Void> s = tair.setCount(ns, key, defaultValue, null);
			assertEquals(ResultCode.OK, s.getCode());
			
			Result<Integer> i = tair.incr(ns, key, value, 2, null);
			assertEquals(ResultCode.OK, i.getCode());
			assertEquals((value + defaultValue), i.getResult());
			 
			 
			
			Result<Integer> d = tair.decr(ns, key, value2, 0, null);
			assertEquals(ResultCode.OK, d.getCode());
			assertEquals((value + defaultValue - value2), d.getResult());
		 
		} catch (TairRpcError e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (TairFlowLimit e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (TairTimeout e) {
			e.printStackTrace();
			assertEquals(true, false);
		} catch (InterruptedException e) {
			e.printStackTrace();
			assertEquals(true, false);
		}
	}
}
