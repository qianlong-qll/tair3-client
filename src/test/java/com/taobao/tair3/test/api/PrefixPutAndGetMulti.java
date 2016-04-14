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
import com.taobao.tair3.client.TairClient.Pair;
import com.taobao.tair3.client.TairClient.RequestOption;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;

public class PrefixPutAndGetMulti extends TestBase {
	@Test
	public void simplePrefixPutAndGetMulti() {
		int keyCount = 20;
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		List<byte[]> skeys = this.generateKeys(keyCount);
		Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
	    for (byte[] key : skeys) {
	    	kvs.put(key, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
	    }
	    try {
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
	public void simplePrefixPutAndPrefixGetMulti() {
		int keyCount = 20;
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		List<byte[]> skeys = this.generateKeys(keyCount);
		try {
		//	tair.prefixInvalidMultiByProxy(ns, pkey, skeys, null);
			for (byte[] skey : skeys) {
				byte[] value = UUID.randomUUID().toString().getBytes();
				Result<Void> p = tair.prefixPut(ns, pkey, skey, value, null);
				assertEquals(ResultCode.OK, p.getCode());
			}
			
			
			ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns, pkey, skeys, null);
			assertEquals(ResultCode.OK, gm.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : gm.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			
			ResultMap<byte[], Result<byte[]>> gm2 = tair.simplePrefixGetMulti(ns, pkey, skeys, null);
			assertEquals(ResultCode.OK, gm2.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : gm2.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
				//System.out.println(" R:  " + entry);
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
}
