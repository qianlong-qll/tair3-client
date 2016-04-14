package com.taobao.tair3.test.api;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
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
import com.taobao.tair3.client.TairClient.RequestOption;
import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.TairConstant;

public class BatchPrefixGetAndGetHidden extends TestBase {
	@Test
	public void simpleBatchGet() {
		int keyCount = 20;
		Map<byte[], List<byte[]>> keys = new HashMap<byte[], List<byte[]>>();
		for (int i = 0; i < keyCount; ++i) {
			byte[] pkey = UUID.randomUUID().toString().getBytes();
			List<byte[]> skeys = new ArrayList<byte[]>();
			for (int x = 0; x < keyCount; x++) {
				skeys.add(UUID.randomUUID().toString().getBytes());
			}
			keys.put(pkey, skeys);
		}

		try {
			for (Map.Entry<byte[], List<byte[]>> e : keys.entrySet()) {
				Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
				byte[] pkey = e.getKey();
				List<byte[]> skeys = e.getValue();
				for (byte[] skey : skeys) {
					kvs.put(skey, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
				}
				ResultMap<byte[], Result<Void>> pm = tair.prefixPutMulti(ns, pkey, kvs, null);
				assertEquals(ResultCode.OK, pm.getCode());
				for (Map.Entry<byte[], Result<Void>> entry : pm.getResult().entrySet()) {
					assertEquals(ResultCode.OK, entry.getValue().getCode());
				}

				ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns,
						pkey, skeys, null);
				assertEquals(ResultCode.OK, gm.getCode());
				for (Map.Entry<byte[], Result<byte[]>> entry : gm.getResult()
						.entrySet()) {
					assertEquals(ResultCode.OK, entry.getValue().getCode());
				}
			}
			
			ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>> bpg = tair.batchPrefixGetMulti(ns, keys, null);
			assertEquals(ResultCode.OK, bpg.getCode());
			for (Map.Entry<byte[], Result<Map<byte[], Result<byte[]>>>> e : bpg.getResult().entrySet()) {
				assertEquals(ResultCode.OK, e.getValue().getCode());
				for (Map.Entry<byte[], Result<byte[]>> x : e.getValue().getResult().entrySet()) {
					assertEquals(ResultCode.OK, x.getValue().getCode());
				}
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
	public void simpleBatchGetHiddenMulti() {
		int keyCount = 20;
		Map<byte[], List<byte[]>> keys = new HashMap<byte[], List<byte[]>>();
		for (int i = 0; i < keyCount; ++i) {
			byte[] pkey = UUID.randomUUID().toString().getBytes();
			List<byte[]> skeys = new ArrayList<byte[]>();
			for (int x = 0; x < keyCount; x++) {
				skeys.add(UUID.randomUUID().toString().getBytes());
			}
			keys.put(pkey, skeys);
		}

		try {
			for (Map.Entry<byte[], List<byte[]>> e : keys.entrySet()) {
				Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
				byte[] pkey = e.getKey();
				List<byte[]> skeys = e.getValue();
				for (byte[] skey : skeys) {
					kvs.put(skey, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
				}
				ResultMap<byte[], Result<Void>> pm = tair.prefixPutMulti(ns, pkey, kvs, null);
				assertEquals(ResultCode.OK, pm.getCode());
				for (Map.Entry<byte[], Result<Void>> entry : pm.getResult().entrySet()) {
					assertEquals(ResultCode.OK, entry.getValue().getCode());
				}

				ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns,
						pkey, skeys, null);
				assertEquals(ResultCode.OK, gm.getCode());
				for (Map.Entry<byte[], Result<byte[]>> entry : gm.getResult()
						.entrySet()) {
					assertEquals(ResultCode.OK, entry.getValue().getCode());
				}
				ResultMap<byte[], Result<Void>> hm = tair.prefixHideMultiByProxy(ns, pkey, skeys, null);
				assertEquals(ResultCode.OK, hm.getCode());
				for (Map.Entry<byte[], Result<Void>> entry : hm.getResult().entrySet()) {
					assertEquals(ResultCode.OK, entry.getValue().getCode());
				}
				ResultMap<byte[], Result<byte[]>> gm1 = tair.prefixGetMulti(ns,
						pkey, skeys, null);
				assertEquals(ResultCode.HIDDEN, gm1.getCode());
				for (Map.Entry<byte[], Result<byte[]>> entry : gm1.getResult()
						.entrySet()) {
					assertEquals(ResultCode.HIDDEN, entry.getValue().getCode());
				}

			}
			TairOption opt = new TairOption(50000, (short)0, 0);
			ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>> bpg = tair.batchPrefixGetHiddenMulti(ns, keys, opt);
			assertEquals(ResultCode.OK, bpg.getCode());
			for (Map.Entry<byte[], Result<Map<byte[], Result<byte[]>>>> e : bpg.getResult().entrySet()) {
				assertEquals(ResultCode.OK, e.getValue().getCode());
				for (Map.Entry<byte[], Result<byte[]>> x : e.getValue().getResult().entrySet()) {
					assertEquals(ResultCode.HIDDEN, x.getValue().getCode());
				}
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
	public void simpleBatchGetHiddenMultiWithIllegalParameters() {
		int keyCount = 20;
		Map<byte[], List<byte[]>> keys = new HashMap<byte[], List<byte[]>>();
		for (int i = 0; i < keyCount; ++i) {
			byte[] pkey = UUID.randomUUID().toString().getBytes();
			List<byte[]> skeys = new ArrayList<byte[]>();
			for (int x = 0; x < keyCount; x++) {
				skeys.add(UUID.randomUUID().toString().getBytes());
			}
			keys.put(pkey, skeys);
		}

		try {
			for (Map.Entry<byte[], List<byte[]>> e : keys.entrySet()) {
				Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
				byte[] pkey = e.getKey();
				List<byte[]> skeys = e.getValue();
				for (byte[] skey : skeys) {
					kvs.put(skey, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
				}
				ResultMap<byte[], Result<Void>> pm = tair.prefixPutMulti(ns, pkey, kvs, null);
				assertEquals(ResultCode.OK, pm.getCode());
				for (Map.Entry<byte[], Result<Void>> entry : pm.getResult().entrySet()) {
					assertEquals(ResultCode.OK, entry.getValue().getCode());
				}

				ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns,
						pkey, skeys, null);
				assertEquals(ResultCode.OK, gm.getCode());
				for (Map.Entry<byte[], Result<byte[]>> entry : gm.getResult()
						.entrySet()) {
					assertEquals(ResultCode.OK, entry.getValue().getCode());
				}
				ResultMap<byte[], Result<Void>> hm = tair.prefixHideMultiByProxy(ns, pkey, skeys, null);
				assertEquals(ResultCode.OK, hm.getCode());
				for (Map.Entry<byte[], Result<Void>> entry : hm.getResult().entrySet()) {
					assertEquals(ResultCode.OK, entry.getValue().getCode());
				}
				ResultMap<byte[], Result<byte[]>> gm1 = tair.prefixGetMulti(ns,
						pkey, skeys, null);
				assertEquals(ResultCode.HIDDEN, gm1.getCode());
				for (Map.Entry<byte[], Result<byte[]>> entry : gm1.getResult()
						.entrySet()) {
					assertEquals(ResultCode.HIDDEN, entry.getValue().getCode());
				}

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
		catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
		
		try {
			ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>> bpg = tair.batchPrefixGetHiddenMulti(ns, null, null);
		} catch (TairRpcError e) {
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
		} catch (TairTimeout e) {
			assertEquals(false, true);
		} catch (InterruptedException e) {
			assertEquals(false, true);
		}
		catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>> bpg = tair.batchPrefixGetHiddenMulti((short)-1, keys, null);
		} catch (TairRpcError e) {
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
		} catch (TairTimeout e) {
			assertEquals(false, true);
		} catch (InterruptedException e) {
			assertEquals(false, true);
		}
		catch (IllegalArgumentException e) {
			assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
		}
		
	}
}
