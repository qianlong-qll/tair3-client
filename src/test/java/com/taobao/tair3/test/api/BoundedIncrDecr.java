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

public class BoundedIncrDecr  extends TestBase { 

	protected static int lowBound = -100;
	protected static int upperBound = 100;
	

	
	@Test
	public void normalIncr() {
		//1. create a counter
		String key = UUID.randomUUID().toString();
	
		
		try {
			tair.invalidByProxy(ns, key.getBytes(), null);
			Result<Integer> i = tair.incr(ns, key.getBytes(), 0, 0, lowBound, upperBound, null);
			assertEquals(ResultCode.OK, i.getCode());
			assertEquals(0, i.getResult());
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
		
		
		
		for (int k = 0; k < upperBound * 2; ++k) {
			try {
				Result<Integer> rr = tair.incr(ns, key.getBytes(), 1, 0, lowBound, upperBound, null);
				//ok
				if (k < upperBound) {
					assertEquals(ResultCode.OK, rr.getCode());
					assertEquals(k + 1, rr.getResult());
				}
				//out of range
				else {
					assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rr.getCode());
				}
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
	
	@Test
	public void normalDecr() {
		//1. create a counter
				String key = UUID.randomUUID().toString();
			
				
				try {
					tair.invalidByProxy(ns, key.getBytes(), null);
					Result<Integer> i = tair.decr(ns, key.getBytes(), 0, 0, lowBound, upperBound, null);
					assertEquals(ResultCode.OK, i.getCode());
					assertEquals(0, i.getResult());
				} catch (TairRpcError e) {
					assertEquals(false, true);
					e.printStackTrace();
				} catch (TairFlowLimit e) {
					assertEquals(false, true);
					e.printStackTrace();
				} catch (TairTimeout e) {
					e.printStackTrace();
					assertEquals(false, true);
					
				} catch (InterruptedException e) {
					assertEquals(false, true);
					e.printStackTrace();
				}
				
				
				
				for (int k = 0; k < upperBound * 2; ++k) {
					try {
						Result<Integer> rr = tair.decr(ns, key.getBytes(), 1, 0, lowBound, upperBound, null);
						//ok
						if (k < upperBound) {
							assertEquals(ResultCode.OK, rr.getCode());
							assertEquals(k - 1, rr.getResult());
						}
						//out of range
						else {
							assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rr.getCode());
						}
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
	
	@Test
	public void boundEqu() {
		String key = UUID.randomUUID().toString();
		 
		int lowBound = 0;
		int upperBound = 0;
		
		try {
			tair.invalidByProxy(ns, key.getBytes(), null);
			Result<Integer> rc = tair.decr(ns, key.getBytes(), 0, 0, lowBound, upperBound, null);
			assertEquals(ResultCode.OK, rc.getCode());
			assertEquals(0, rc.getResult());
			
			Result<Integer> rc1 = tair.decr(ns, key.getBytes(), 1, 0, lowBound, upperBound, null);
			assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rc1.getCode());
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
	public void boundNeg() {
		String key = UUID.randomUUID().toString();
		int lowBound = 10;
		int upperBound = -10;
		try {
			tair.invalidByProxy(ns, key.getBytes(), null);
			Result<Integer> rc = tair.decr(ns, key.getBytes(), 0, 0, lowBound, upperBound, null);
			//assertEquals(ResultCode.INVALID_ARGUMENT, rc.getCode());
			
			//Result<Integer> rc1 = tair.incr(namespace, key, 0, 0, 0, lowBound, upperBound);
			//assertEquals(rc1.getRc(), ResultCode.SERIALIZEERROR);
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
			assertEquals(true, true);
		}
		//assertEquals(ResultCode.);
		
		//Result<Integer> rc1 = tair.incr(namespace, key, 0, 0, 0, lowBound, upperBound);
		//assertEquals(rc1.getRc(), ResultCode.SERIALIZEERROR);
	}
	
	@Test
	public void ExistKeyDecr() {
		String key = UUID.randomUUID().toString();
		//removeKey(key);
		Integer x = 0;
		try {
			tair.invalidByProxy(ns, key.getBytes(), null);
			Result<Void> rp = tair.put(ns, key.getBytes(), x.toString().getBytes(), null);
			assertEquals(ResultCode.OK, rp.getCode());
			
			Result<byte[]> rg = tair.get(ns, key.getBytes(), null);
			assertEquals(ResultCode.OK, rg.getCode());
			//assertEquals(x.toString().getBytes(), rg.getResult());
			
			assertEquals(ResultCode.OK, tair.setCount(ns, key.getBytes(), 0, null).getCode());
			for (int i = 0; i < upperBound * 2; ++i) {
				Result<Integer> rr = tair.decr(ns, key.getBytes(), 1, 0, lowBound, upperBound, null);
				if (i < upperBound) {
					assertEquals(ResultCode.OK, rr.getCode());
					assertEquals(i - 1, rr.getResult());
				}
				//out of range
				else {
					assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rr.getCode());
				}
			}
		} catch (TairRpcError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairTimeout e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void ExistKeyIncr() {
		String key = UUID.randomUUID().toString();
		//removeKey(key);
		Integer x = 0;
		try {
			tair.invalidByProxy(ns, key.getBytes(), null);
			Result<Void> rp = tair.put(ns, key.getBytes(), x.toString().getBytes(), null);
			assertEquals(ResultCode.OK, rp.getCode());
			
			Result<byte[]> rg = tair.get(ns, key.getBytes(), null);
			assertEquals(ResultCode.OK, rg.getCode());
			//assertEquals(x.toString().getBytes(), rg.getResult());
			
			assertEquals(ResultCode.OK, tair.setCount(ns, key.getBytes(), 0, null).getCode());
			for (int i = 0; i < upperBound * 2; ++i) {
				Result<Integer> rr = tair.incr(ns, key.getBytes(), 1, 0, lowBound, upperBound, null);
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairTimeout e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
