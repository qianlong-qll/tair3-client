package com.taobao.tair3.test.api;

import static org.junit.Assert.*;
import java.util.UUID;
import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairConstant;
public class PrefixPut extends TestBase {
	
	@Test
	public void simplePrefixPut() {
		String _pkey = UUID.randomUUID().toString();
		String _skey = UUID.randomUUID().toString();
		String _val = UUID.randomUUID().toString();
		byte[] pkey = _pkey.getBytes();
		byte[] skey = _skey.getBytes();
		byte[] val = _val.getBytes();
		 
		try {
			Result<Void> r = tair.prefixPut(ns, pkey, skey, val, opt);
			assertEquals(ResultCode.OK, r.getCode());
			 
			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, null);
			assertEquals(ResultCode.OK, g.getCode());
			
			//assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
			assertEquals(new ByteArray(val), new ByteArray(g.getResult()));
			assertEquals(1, g.getVersion());
			//assertEquals(0, g.getFlag());
			 
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
	public void simplePutWithIllegalParameter() {
		
		try {
			byte[] pkey = UUID.randomUUID().toString().getBytes();
			byte[] skey = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.prefixPut(ns, null, skey, val, opt);
			
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
		}catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			byte[] pkey = UUID.randomUUID().toString().getBytes();
			byte[] skey = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.prefixPut(ns, pkey, null, val, opt);
			
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
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.VALUE_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			byte[] key = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.prefixPut(ns, null, null, val, opt);
			
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
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			byte[] pkey = UUID.randomUUID().toString().getBytes();
			byte[] skey = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.prefixPut(ns, pkey, skey, null, opt);
			
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
		}  catch (IllegalArgumentException e) {
			assertEquals(TairConstant.VALUE_NOT_AVAILABLE, e.getMessage());
		}
		
		try {
			byte[] pkey = UUID.randomUUID().toString().getBytes();
			byte[] skey = UUID.randomUUID().toString().getBytes();
			byte[] val = UUID.randomUUID().toString().getBytes();
			Result<Void> r = tair.prefixPut((short)-1, pkey, skey, val, opt);
			
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
		} catch (IllegalArgumentException e) {
			assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
		}
	}
	
	@Test
	public void PutWithVersion() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		byte[] value = UUID.randomUUID().toString().getBytes();
		try {
			TairOption opt = new TairOption(500, (short)0, 0);
			Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.OK, r.getCode());
			 
			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, opt);
			assertEquals(ResultCode.OK, g.getCode());
			
			//assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
			assertEquals(new ByteArray(value), new ByteArray(g.getResult()));
			assertEquals(1, g.getVersion());
			//assertEquals(0, g.getFlag());
			
			
			opt.setVersion((short)4);
			Result<Void> r1 = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.VERSION_ERROR, r1.getCode());
			
			opt.setVersion((short)1);
			Result<Void> r2 = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.OK, r2.getCode());
			
			Result<byte[]> g1 =  tair.prefixGet(ns, pkey, skey, opt);
			assertEquals(ResultCode.OK, g1.getCode());
			
		 
			assertEquals(new ByteArray(value), new ByteArray(g1.getResult()));
			assertEquals(2, g1.getVersion());
			//assertEquals(0, g1.getFlag());
			 
			
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
	
	@Test
	public void PutWithExipred() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		byte[] value = UUID.randomUUID().toString().getBytes();
		try {
			TairOption opt = new TairOption(500, (short) 0, 2);
			Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
			assertEquals(ResultCode.OK, r.getCode());

			Thread.sleep(3000);
			 
			Result<byte[]> g = tair.prefixGet(ns, pkey, skey, opt);
			assertEquals(ResultCode.NOTEXISTS, g.getCode());
			 
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
