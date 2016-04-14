package com.taobao.tair3.test.api;

import static org.junit.Assert.*;
import java.util.UUID;
import org.junit.Test;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.TairConstant;

public class BoundedPrefixIncrDecr  extends TestBase {
	 
	protected static int lowBound = -100;
	protected static int upperBound = 100;
	@Test
	public void normalIncr() {
		//1. create a counter
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		int value = 0;
		int defaultValue = 0;
		removeKey(pkey, skey);
		try {
			Result<Integer> ic = tair.prefixIncr(ns, pkey, skey, value, defaultValue, lowBound, upperBound, null);
			assertEquals(ResultCode.OK, ic.getCode());
			assertEquals(value + defaultValue, ic.getResult());
			
			
			//2.incr upperBound times
			for (int i = 0; i < upperBound * 2; ++i) {
				Result<Integer> rr = tair.prefixIncr(ns, pkey, skey, 1, defaultValue, lowBound, upperBound, null);
				//ok
				if (i < upperBound) {
					assertEquals(ResultCode.OK, rr.getCode());
					assertEquals(i + 1, rr.getResult());
				}
				//out of range
				else {
					assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rr.getCode());
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
	public void normalDecr() {
		//1. create a counter
				byte[] pkey = UUID.randomUUID().toString().getBytes();
				byte[] skey = UUID.randomUUID().toString().getBytes();
				int value = 0;
				int defaultValue = 0;
				removeKey(pkey, skey);
				try {
					Result<Integer> ic = tair.prefixDecr(ns, pkey, skey, value, defaultValue, lowBound, upperBound, null);
					assertEquals(ResultCode.OK, ic.getCode());
					assertEquals(- value + defaultValue, ic.getResult());
					
					
					//2.incr upperBound times
					for (int i = 0; i < upperBound * 2; ++i) {
						Result<Integer> rr = tair.prefixDecr(ns, pkey, skey, 1, defaultValue, lowBound, upperBound, null);
						//ok
						if (i < upperBound) {
							assertEquals(ResultCode.OK, rr.getCode());
							assertEquals(-i - 1, rr.getResult());
						}
						//out of range
						else {
							assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rr.getCode());
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
	public void boundEqu() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		int value = 10;
		int defaultValue = 0;
		int lowBound = 0;
		int upperBound = 0;
		removeKey(pkey, skey);
		try {
			Result<Integer> ic = tair.prefixDecr(ns,pkey, skey, value, defaultValue, lowBound, upperBound, null);
			assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, ic.getCode());
		} catch (TairRpcError e) {
			assertEquals(false, true);
		} catch (TairFlowLimit e) {
			assertEquals(false, true);
		} catch (TairTimeout e) {
			assertEquals(false, true);
		} catch (InterruptedException e) {
			assertEquals(false, true);
		}
		try {
			Result<Integer> ic = tair.prefixIncr(ns,pkey, skey, value, defaultValue, lowBound, upperBound, null);
			assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, ic.getCode());
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
	public void boundNeg() {
		byte[] pkey = UUID.randomUUID().toString().getBytes();
		byte[] skey = UUID.randomUUID().toString().getBytes();
		int value = 10;
		int defaultValue = 0;
		int lowBound = 10;
		int upperBound = -10;
		removeKey(pkey, skey);
		try {
			Result<Integer> ic = tair.prefixIncr(ns,pkey, skey, value, defaultValue, lowBound, upperBound, null);
			assertEquals(true, false);
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
			Result<Integer> ic = tair.prefixDecr(ns,pkey, skey, value, defaultValue, lowBound, upperBound, null);
			assertEquals(true, false);
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
