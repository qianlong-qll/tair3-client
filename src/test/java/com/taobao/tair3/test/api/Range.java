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
import com.taobao.tair3.client.TairClient.Pair;
import com.taobao.tair3.client.TairClient.RequestOption;
import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;

public class Range extends TestBase {
	@Test
	public void simpelGetRange() {
		int keyCount = 20;
		byte[] pkey =   UUID.randomUUID().toString().getBytes();
		List<byte[]> skeys = this.generateOrderedKeys(UUID.randomUUID().toString().getBytes(), keyCount);
		Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
	    for (byte[] key : skeys) {
	    	kvs.put(key, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
	    }
	    try {
	    	TairOption opt = new TairOption(500, (short)0, 0);
			ResultMap<byte[], Result<Void>> pm = tair.prefixPutMulti(ns, pkey, kvs, null);
			assertEquals(ResultCode.OK, pm.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : pm.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			
			ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns, pkey, skeys, null);
			assertEquals(ResultCode.OK, gm.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : gm.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			
			byte[] start = skeys.get(0);
			byte[] end = skeys.get(keyCount - 1);
			
			Result<List<Pair<byte[], Result<byte[]>>>> r = tair.getRange(ns, pkey, start, end, 0, keyCount, false, opt);
			assertEquals(ResultCode.OK, r.getCode());
		//	for (Map.Entry<byte[], Result<byte[]>> e : r.getResult().entrySet()) {
		//		assertEquals(ResultCode.OK, e.getValue().getCode());
		//	}
			
			Result<List<Pair<byte[], Result<byte[]>>>> r1 = tair.getRange(ns, pkey, null, null, 0, keyCount, false, opt);
			assertEquals(ResultCode.OK, r1.getCode());
		//	for (Map.Entry<byte[], Result<byte[]>> e : r1.getResult().entrySet()) {
		//		assertEquals(ResultCode.OK, e.getValue().getCode());
		//	}
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
	public void simpelGetRangeKey() {
		int keyCount = 9;
		byte[] pkey =   UUID.randomUUID().toString().getBytes();
		List<byte[]> skeys = this.generateOrderedKeys(UUID.randomUUID().toString().getBytes(), keyCount);
		Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
	    for (byte[] key : skeys) {
	    	kvs.put(key, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
	    }
	    try {
	    	TairOption opt = new TairOption(500, (short)0, 0);
			ResultMap<byte[], Result<Void>> pm = tair.prefixPutMulti(ns, pkey, kvs, null);
			assertEquals(ResultCode.OK, pm.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : pm.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			
			ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns, pkey, skeys, null);
			assertEquals(ResultCode.OK, gm.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : gm.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
		 
			byte[] start = skeys.get(0);
			byte[] end = skeys.get(keyCount - 1);
			Result<List<Result<byte[]>>> r = tair.getRangeKey(ns, pkey, start, end, 0, keyCount, false, opt);
			assertEquals(ResultCode.OK, r.getCode());
			//assertEquals(keyCount, r.getResult().size());
			
			Result<List<Result<byte[]>>> r1 = tair.getRangeKey(ns, pkey, null, null, 0, keyCount, false, opt);
			assertEquals(ResultCode.OK, r1.getCode());
			assertEquals(keyCount, r1.getResult().size());
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
	public void simpelGetRangeValue() {
		int keyCount = 20;
		byte[] pkey =   UUID.randomUUID().toString().getBytes();
		List<byte[]> skeys = this.generateOrderedKeys(UUID.randomUUID().toString().getBytes(), keyCount);
		Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
	    for (byte[] key : skeys) {
	    	kvs.put(key, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
	    }
	    try {
	    	TairOption opt = new TairOption(500, (short)0, 0);
			ResultMap<byte[], Result<Void>> pm = tair.prefixPutMulti(ns, pkey, kvs, null);
			assertEquals(ResultCode.OK, pm.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : pm.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			
			ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns, pkey, skeys, null);
			assertEquals(ResultCode.OK, gm.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : gm.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
		 
			
			byte[] start = skeys.get(0);
			byte[] end = skeys.get(keyCount - 1);
			Result<List<Result<byte[]>>> r = tair.getRangeValue(ns, pkey, start, end, 0, keyCount, false, opt);
			assertEquals(ResultCode.OK, r.getCode());
			//assertEquals(keyCount, r.getResult().size());
			
			Result<List<Result<byte[]>>> r1 = tair.getRangeValue(ns, pkey, null, null, 0, keyCount, false, opt);
			assertEquals(ResultCode.OK, r1.getCode());
			//assertEquals(keyCount, r1.getResult().size());
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
}
