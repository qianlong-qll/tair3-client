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
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.TairConstant;

public class PrefixHideAndGetHiddenMulti extends TestBase {
	@Test
	public void simplePrefixHideAndGetHiddenMulti() {
		int keyCount = 6;
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		List<byte[]> skeys = /*this.generateKeys(keyCount)*/ new ArrayList<byte[]> ();
		skeys.add("Key1".getBytes());
		skeys.add("Key2".getBytes());
		skeys.add("Key3".getBytes());
		skeys.add("Key4".getBytes());
		skeys.add("Key4".getBytes());
		skeys.add("Key5".getBytes());
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
			
			ResultMap<byte[], Result<Void>> hm = tair.prefixHideMultiByProxy(ns, pkey, skeys, null);
			assertEquals(ResultCode.OK, hm.getCode());
			for (Map.Entry<byte[], Result<Void>> entry : hm.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			
			ResultMap<byte[], Result<byte[]>> gm1 = tair.prefixGetMulti(ns, pkey, skeys, null);
			assertEquals(ResultCode.HIDDEN, gm1.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : gm1.getResult().entrySet()) {
				assertEquals(ResultCode.HIDDEN, entry.getValue().getCode());
			}
			
			ResultMap<byte[], Result<byte[]>> ghm = tair.prefixGetHiddenMulti(ns, pkey, skeys, null);
			assertEquals(ResultCode.OK, ghm.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : ghm.getResult().entrySet()) {
				assertEquals(ResultCode.HIDDEN, entry.getValue().getCode());
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
	public void simpleJustPrefixGetHiddenMulti() {
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
			
			 
			
			ResultMap<byte[], Result<byte[]>> gm1 = tair.prefixGetMulti(ns, pkey, skeys, null);
			assertEquals(ResultCode.OK, gm1.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : gm1.getResult().entrySet()) {
				assertEquals(ResultCode.OK, entry.getValue().getCode());
			}
			
			ResultMap<byte[], Result<byte[]>> ghm = tair.prefixGetHiddenMulti(ns, pkey, skeys, null);
			assertEquals(ResultCode.OK, ghm.getCode());
			for (Map.Entry<byte[], Result<byte[]>> entry : ghm.getResult().entrySet()) {
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
	public void simpleJustPrefixGetHiddenMultiWithIllegalParameter() {
		int keyCount = 20;
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		List<byte[]> skeys = this.generateKeys(keyCount);
		Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
	    for (byte[] key : skeys) {
	    	kvs.put(key, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
	    }
	    try {
			 
			
	    	tair.prefixGetHiddenMulti(ns, null, skeys, null);
			assertEquals(false, true);
			
		 
			
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
	    	tair.prefixGetHiddenMulti(ns, pkey, null, null);
			assertEquals(false, true);
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
	    	tair.prefixGetHiddenMulti((short)-1, pkey, skeys, null);
			assertEquals(false, true);
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
	
	@Test
	public void simpleJustPrefixHideMultiWithIllegalParameter() {
		int keyCount = 20;
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		List<byte[]> skeys = this.generateKeys(keyCount);
		Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
	    for (byte[] key : skeys) {
	    	kvs.put(key, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
	    }
	    
	    try {
	    	tair.prefixHideMultiByProxy(ns, null, skeys, null);
	    	assertEquals(false, true);
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
	    	tair.prefixHideMultiByProxy(ns, pkey, null, null);
	    	assertEquals(false, true);
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
	    	tair.prefixHideMultiByProxy((short)-1, pkey, skeys, null);
	    	assertEquals(false, true);
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
