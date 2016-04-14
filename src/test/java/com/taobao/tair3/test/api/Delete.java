package com.taobao.tair3.test.api;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
public class Delete extends TestBase {
	@Test
	public void simpleDelete() {
		////test case
		ReentrantReadWriteLock c = new ReentrantReadWriteLock();
		for (int i = 0 ; i < 10; i++) {
			c.readLock().lock();
			System.out.println(c.readLock());
		}
		for (int i = 0 ; i < 10; i++) {
			c.readLock().unlock();
		}
		////test case end
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
			
			Result<Void> d = tair.invalidByProxy(ns, key, null);
			assertEquals(ResultCode.OK, d.getCode());
			
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
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		try {
			//Result<Void> r = tair.put(ns, key, val, null);
			//assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.get(ns, key, null);
			assertEquals(ResultCode.NOTEXISTS, g.getCode());
			
			Result<Void> d = tair.invalidByProxy(ns, key, null);
			assertEquals(ResultCode.NOTEXISTS, d.getCode());
			
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
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] val = UUID.randomUUID().toString().getBytes();
		try {
			Result<Void> r = tair.put(ns, key, val, new TairOption(500, (short)0, 2));
			assertEquals(ResultCode.OK, r.getCode());
			
			Thread.sleep(3000);
			
			Result<byte[]> g = tair.get(ns, key, null);
			assertEquals(ResultCode.NOTEXISTS, g.getCode());
			
			Result<Void> d = tair.invalidByProxy(ns, key, null);
			assertEquals(ResultCode.NOTEXISTS, d.getCode());
			
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
	}
}
