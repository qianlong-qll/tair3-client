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
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairConstant;
public class LockAndUnlock extends TestBase {
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
	
	@Test
	public void simpleLockTwice() {
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
			
			Result<Void> u = tair.lock(ns, key, null);
			assertEquals(ResultCode.LOCK_ALREADY_EXIST, u.getCode());
			
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
	
	@Test
	public void simpleUnlockTwice() {
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
			
			Result<Void> u1 = tair.unlock(ns, key, null);
			assertEquals(ResultCode.LOCK_NOT_EXIST, u1.getCode());
			
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
	
	@Test
	public void simpleLockNotExist() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		try {
			Result<Void> d = tair.invalidByProxy(ns, key, null);
			assertEquals(ResultCode.NOTEXISTS, d.getCode());
			
			Result<Void> l = tair.lock(ns, key, null);
			assertEquals(ResultCode.NOTEXISTS, l.getCode());
			
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
	
	@Test
	public void simpleGetAfterLock() {
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
			
			
			Result<Void> l = tair.lock(ns, key, null);
			assertEquals(ResultCode.OK, l.getCode());
			
			Result<byte[]> g1 = tair.get(ns, key, null);
			assertEquals(ResultCode.OK, g1.getCode());
			
			assertEquals(new ByteArray(key), new ByteArray(g1.getKey()));
			assertEquals(new ByteArray(val), new ByteArray(g1.getResult()));
			assertEquals(2, g1.getVersion());
			//assertEquals(true, g1.isLocked());
			
		
			
			Result<Void> u = tair.unlock(ns, key, null);
			assertEquals(ResultCode.OK, u.getCode());
			
			Result<Void> u1 = tair.unlock(ns, key, null);
			assertEquals(ResultCode.LOCK_NOT_EXIST, u1.getCode());
			
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
	
	@Test
	public void simpleLockCounter() {
		byte[] key = UUID.randomUUID().toString().getBytes();
		try {
			int count = 10;
			Result<Void> d = tair.invalidByProxy(ns, key, null);
			assertEquals(ResultCode.NOTEXISTS, d.getCode());
			
			Result<Integer> i = tair.incr(ns, key, count, 0, null);
			assertEquals(ResultCode.OK, i.getCode());
			
			
			Result<Void> l = tair.lock(ns, key, null);
			assertEquals(ResultCode.OK, l.getCode());
			
			Result<Integer> i1 = tair.incr(ns, key, count, 0, null);
			assertEquals(ResultCode.LOCK_ALREADY_EXIST, i1.getCode());
			
			
			Result<Void> u = tair.unlock(ns, key, null);
			assertEquals(ResultCode.OK, u.getCode());
			
			Result<Integer> i2 = tair.incr(ns, key, count, 0, null);
			assertEquals(ResultCode.OK, i2.getCode());

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
	
	@Test
	public void simpleLockAndUnlockWithIllegalParameter() {
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
			
			Result<Void> l = tair.lock(ns, null, null);
			assertEquals(ResultCode.OK, l.getCode());
			
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
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			Result<Void> r = tair.put(ns, key, val, null);
			assertEquals(ResultCode.OK, r.getCode());
			
			Result<byte[]> g = tair.get(ns, key, null);
			assertEquals(ResultCode.OK, g.getCode());
			
			assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
			assertEquals(new ByteArray(val), new ByteArray(g.getResult()));
			//assertEquals(1, g.getVersion());
			//assertEquals(0, g.getFlag());
			
			Result<Void> l = tair.lock(ns, key, null);
			assertEquals(ResultCode.OK, l.getCode());
			
			Result<Void> u = tair.unlock(ns, null, null);
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
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
	}
}
