package com.taobao.tair3.test.api;

import static org.junit.Assert.assertEquals;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.TairClient.Counter;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;

public class BoundedPrefixIncrDecrMulti  extends TestBase {
 
	protected static int lowBound = -100;
	protected static int upperBound = 100;

	@Test
	public void normalIncr() {
		//1. create a counter
		int keyCount = 10;
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		List<byte[]> skeys = this.generateKeys(keyCount);
		removeKey(pkey, skeys);
		Map<byte[], Counter> skv = new HashMap<byte[], Counter>();
		for (byte[] skey : skeys) {
			skv.put(skey, new Counter(0,0,0));
		}
		try {
			ResultMap<byte[], Result<Integer>> pi = tair.prefixIncrMulti(ns, pkey, skv, opt);
			assertEquals(ResultCode.OK, pi.getCode());
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
