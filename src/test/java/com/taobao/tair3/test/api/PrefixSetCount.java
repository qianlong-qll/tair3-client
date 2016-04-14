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
public class PrefixSetCount extends TestBase {
	@Test
	public void simpleSetCount() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		try {
			int value = 1;
			int defaultValue = 1;
			Result<Void> rd = tair.prefixInvalidByProxy(ns, pkey, skey, null);
			assertEquals(true , rd.getCode().equals(ResultCode.OK) || rd.getCode().equals(ResultCode.NOTEXISTS));
			
			Result<Void> rs = tair.prefixSetCount(ns, pkey, skey, defaultValue, null);
			assertEquals(ResultCode.OK, rs.getCode());
			
			Result<byte[]> rg = tair.prefixGet(ns, pkey, skey, opt);
			assertEquals(ResultCode.OK, rg.getCode());
			assertEquals(true, rg.isCounter());
			
			Result<Integer> i = tair.prefixIncr(ns, pkey, skey, value, defaultValue, null);
			assertEquals(ResultCode.OK, i.getCode());
			assertEquals((value + defaultValue), i.getResult());
			
			Result<Integer> d = tair.prefixDecr(ns, pkey, skey, value, defaultValue, opt);
			assertEquals(ResultCode.OK, d.getCode());
			assertEquals((defaultValue), d.getResult());
			
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
