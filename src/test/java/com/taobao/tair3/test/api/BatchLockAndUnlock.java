package com.taobao.tair3.test.api;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.TairConstant;

public class BatchLockAndUnlock extends TestBase {
	@Test
	public void simpleBatchLockAndUnlock() {
		List<byte[]> keys = generateKeys(10);
		Map<byte[], byte[]> kvs = new HashMap<byte[], byte[]>();
		for (byte[] key : keys) {
			kvs.put(key, UUID.randomUUID().toString().getBytes());
		}
		try {
			//batch put
			ResultMap<byte[], Result<Void>> bp = tair.batchPut(ns, kvs, null);
			assertEquals(ResultCode.OK, bp.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			//batch get
			ResultMap<byte[], Result<byte[]>> bg = tair.batchGet(ns, keys, null);
			assertEquals(ResultCode.OK, bg.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			//batch lock
			ResultMap<byte[], Result<Void>> bl = tair.batchLock(ns, keys, null);
			assertEquals(ResultCode.OK, bl.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bl.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			//batch get
			ResultMap<byte[], Result<byte[]>> bg1 = tair.batchGet(ns, keys, null);
			assertEquals(ResultCode.OK, bg1.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : bg1.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			
			//batch lock again
			ResultMap<byte[], Result<Void>> bl1 = tair.batchLock(ns, keys, null);
			assertEquals(ResultCode.LOCK_ALREADY_EXIST, bl1.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bl1.getResult().entrySet()) {
				assertEquals(ResultCode.LOCK_ALREADY_EXIST, entry.getValue().getCode());
			}
			
			//batch lock
			ResultMap<byte[], Result<Void>> bul = tair.batchUnlock(ns, keys, null);
			assertEquals(ResultCode.OK, bul.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bul.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			//batch get
			ResultMap<byte[], Result<byte[]>> bg2 = tair.batchGet(ns, keys, null);
			assertEquals(ResultCode.OK, bg2.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : bg2.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			
			//batch lock
			ResultMap<byte[], Result<Void>> bul1 = tair.batchUnlock(ns, keys, null);
			assertEquals(ResultCode.LOCK_NOT_EXIST, bul1.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bul1.getResult().entrySet()) {
				assertEquals(ResultCode.LOCK_NOT_EXIST, entry.getValue().getCode());
			}
		} catch (TairRpcError e) {
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
		} catch (TairTimeout e) {
			assertEquals(false, true);
		} catch (InterruptedException e) {
			assertEquals(false, true);
		}
	}
	
	@Test
	public void simpleBatchLockAndUnlockWithIllegalParameter() {
		List<byte[]> keys = generateKeys(10);
		Map<byte[], byte[]> kvs = new HashMap<byte[], byte[]>();
		for (byte[] key : keys) {
			kvs.put(key, UUID.randomUUID().toString().getBytes());
		}
		try {
			//batch put
			ResultMap<byte[], Result<Void>> bp = tair.batchPut(ns, kvs, null);
			assertEquals(ResultCode.OK, bp.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			//batch get
			ResultMap<byte[], Result<byte[]>> bg = tair.batchGet(ns, keys, null);
			assertEquals(ResultCode.OK, bg.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			//batch lock
			ResultMap<byte[], Result<Void>> bl = tair.batchLock(ns, null, null);
			
		} catch (TairRpcError e) {
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
		} catch (TairTimeout e) {
			assertEquals(false, true);
		} catch (InterruptedException e) {
			assertEquals(false, true);
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			//batch put
			ResultMap<byte[], Result<Void>> bp = tair.batchPut(ns, kvs, null);
			assertEquals(ResultCode.OK, bp.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			//batch get
			ResultMap<byte[], Result<byte[]>> bg = tair.batchGet(ns, keys, null);
			assertEquals(ResultCode.OK, bg.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			//batch lock
			ResultMap<byte[], Result<Void>> bl = tair.batchLock(ns, keys, null);
			
			//batch lock
			ResultMap<byte[], Result<Void>> bl1 = tair.batchUnlock(ns, null, null);
			
		} catch (TairRpcError e) {
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
		} catch (TairTimeout e) {
			assertEquals(false, true);
		} catch (InterruptedException e) {
			assertEquals(false, true);
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
	}
}
