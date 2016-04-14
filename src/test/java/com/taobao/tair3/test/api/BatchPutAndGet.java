package com.taobao.tair3.test.api;

import static org.junit.Assert.*;

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

public class BatchPutAndGet extends TestBase {
	protected int keyCount = 20;
	@Test
	public void simpleBatchPutAndGet() {
		List<byte[]> keys = generateKeys(10);
		Map<byte[], byte[]> kvs = new HashMap<byte[], byte[]>();
		for (byte[] key : keys) {
			kvs.put(key, UUID.randomUUID().toString().getBytes());
		}
		try {
			ResultMap<byte[], Result<Void>> bp = tair.batchPut(ns, kvs, null);
			assertEquals(ResultCode.OK, bp.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			
			ResultMap<byte[], Result<byte[]>> bg = tair.batchGet(ns, keys, null);
			assertEquals(ResultCode.OK, bg.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
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
	public void simpleBatchPutWithIllegalInput() {
		List<byte[]> keys = generateKeys(10);
		Map<byte[], byte[]> kvs = new HashMap<byte[], byte[]>();
		for (byte[] key : keys) {
			kvs.put(key, null);
		}
		try {
			ResultMap<byte[], Result<Void>> bp = tair.batchPut(ns, null, null);
			assertEquals(ResultCode.OK, bp.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
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
			ResultMap<byte[], Result<Void>> bp = tair.batchPut((short)-1, kvs, null);
			assertEquals(ResultCode.OK, bp.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
		} catch (TairRpcError e) {
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
		} catch (TairTimeout e) {
			assertEquals(false, true);
		} catch (InterruptedException e) {
			assertEquals(false, true);
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			ResultMap<byte[], Result<Void>> bp = tair.batchPut(ns, kvs, null);
			assertEquals(ResultCode.OK, bp.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
		} catch (TairRpcError e) {
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
		} catch (TairTimeout e) {
			assertEquals(false, true);
		} catch (InterruptedException e) {
			assertEquals(false, true);
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.VALUE_NOT_AVAILABLE, e.getMessage());
		}
	}
	
	@Test
	public void simpleBatchGetWithIllegalInput() {
		List<byte[]> keys = generateKeys(10);
		Map<byte[], byte[]> kvs = new HashMap<byte[], byte[]>();
		for (byte[] key : keys) {
			kvs.put(key, null);
		}
		try {
			ResultMap<byte[], Result<byte[]>> bg = tair.batchGet(ns, null, null);
			assertEquals(ResultCode.OK, bg.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
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
			ResultMap<byte[], Result<byte[]>> bg = tair.batchGet((short)-1, keys, null);
			assertEquals(ResultCode.OK, bg.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
		} catch (TairRpcError e) {
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
		} catch (TairTimeout e) {
			assertEquals(false, true);
		} catch (InterruptedException e) {
			assertEquals(false, true);
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
		}
	}
}
