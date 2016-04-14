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
import com.taobao.tair3.client.util.TairConstant;

public class PrefixDeleteMulti extends TestBase {
	@Test
	public void simplePrefixDeleteMulti() {
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
			
			ResultMap<byte[], Result<Void>> dm = tair.prefixInvalidMultiByProxy(ns, pkey, skeys, null);
			assertEquals(ResultCode.OK, dm.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : dm.getResult().entrySet()) {
				assertEquals(true, ResultCode.OK.equals(entry.getValue().getCode()) || ResultCode.NOTEXISTS.equals(entry.getValue().getCode()));
			}
			
			ResultMap<byte[], Result<byte[]>> gm1 = tair.prefixGetMulti(ns, pkey, skeys, null);
			assertEquals(ResultCode.NOTEXISTS, gm1.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : gm1.getResult().entrySet()) {
				assertEquals(true, ResultCode.OK.equals(entry.getValue().getCode()) || ResultCode.NOTEXISTS.equals(entry.getValue().getCode()));
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
	public void prefixDeleteMultWithIllegalParameter() {
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
			
			ResultMap<byte[], Result<Void>> dm = tair.prefixInvalidMultiByProxy(ns, null, skeys, null);
			assertEquals(ResultCode.OK, dm.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : dm.getResult().entrySet()) {
				assertEquals(true, ResultCode.OK.equals(entry.getValue().getCode()) || ResultCode.NOTEXISTS.equals(entry.getValue().getCode()));
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
			
			ResultMap<byte[], Result<Void>> dm = tair.prefixInvalidMultiByProxy(ns, pkey, null, null);
			assertEquals(ResultCode.OK, dm.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : dm.getResult().entrySet()) {
				assertEquals(true, ResultCode.OK.equals(entry.getValue().getCode()) || ResultCode.NOTEXISTS.equals(entry.getValue().getCode()));
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
			
			ResultMap<byte[], Result<Void>> dm = tair.prefixInvalidMultiByProxy((short)-1, pkey, skeys, null);
			assertEquals(ResultCode.OK, dm.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : dm.getResult().entrySet()) {
				assertEquals(true, ResultCode.OK.equals(entry.getValue().getCode()) || ResultCode.NOTEXISTS.equals(entry.getValue().getCode()));
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
