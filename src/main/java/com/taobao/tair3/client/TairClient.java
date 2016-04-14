package com.taobao.tair3.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairQueueOverflow;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;


public interface TairClient {
	
	public static class TairOption {
		private RequestOption requestOption;
		private long timeout;
		
		public TairOption(long timeout, short version, int expire) {
			this.requestOption = new RequestOption(version, expire);
			this.timeout = timeout;
		}
		public TairOption(long timeout, short version) {
			 this(timeout, version, 0);
		}
		public TairOption(long timeout) {
			this(timeout, (short)0, 0);
		}
		public TairOption() {
			this(0, (short)0, 0);
		}
		
		public RequestOption getRequestOption() {
			return this.requestOption;
		}
		public void setTimeout(long timeout) {
			this.timeout = timeout;
		}
		public void setVersion(short version) {
			this.requestOption.version = version;
		}
		public void setExpireTime(int expire) {
			this.requestOption.expire = expire;
		}
		public long getTimeout() {
			return timeout;
		}
		public short getVersion() {
			return requestOption.version;
		}
		public int getExpire() {
			return requestOption.expire;
		}
	}

	public static class RequestOption {
		private short version;
		private int expire;
		public void setVersion(short version) {
			this.version = version;
		}
		public void setExpire(int expire) {
			this.expire = expire;
		}
		public short getVersion() {
			return version;
		}
		public int getExpire() {
			return expire;
		}
		public RequestOption() {
			this.version = 0;
			this.expire = 0;
		}
		public RequestOption(short version, int expire) {
			this.version = version;
			this.expire = expire;
		}
	}

	public static class Counter {
		private int value = 0;
		private int initValue = 0;
		private int expire = 0;
		public Counter() {
			
		}
		public Counter(int value, int initValue, int expire) {
			this.value = value;
			this.initValue = initValue;
			this.expire = expire;
		}
		public int getValue() {
			return value;
		}
		public int getInitValue() {
			return initValue;
		}
		public int getExpire() {
			return expire;
		}
		public void setExpire(int expire) {
			this.expire = expire;
		}
		public void negated() {
			this.value = - this.value;
		}
	}

	public static class Pair<F, S> {
		protected F first = null;
		protected S second = null;
		public Pair(F first, S second) {
			this.first = first;
			this.second = second;
		}
		public F first() {
			return first;
		}
		public S second() {
			return second;
		}
		public boolean isAvaliable() {
			return first != null && second != null;
		}
	}
	
	public Future<Result<Void>> putAsync(short ns, byte[] key, byte[] value, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<byte[]>> getAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit ;
	
	public Future<ResultMap<byte[], Result<Void>>> batchPutAsync(short ns, final Map<byte[], byte[]> kv, TairOption opt) throws TairRpcError, TairFlowLimit;

	public Future<ResultMap<byte[], Result<byte[]>>> batchGetAsync(short ns, final List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<Void>> setCountAsync(short ns, byte[] key, int count, TairOption opt) throws TairRpcError, TairFlowLimit;
	 
	public Future<Result<Integer>> incrAsync(short ns, byte[] key, int value, int defaultValue, TairOption opt) throws TairRpcError, TairFlowLimit;
	 
	public Future<Result<Integer>> decrAsync(short ns, byte[] key, int value, int defaultValue, TairOption opt) throws TairRpcError, TairFlowLimit;
	 
	public Future<Result<Integer>> incrAsync(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit;
	 
	public Future<Result<Integer>> decrAsync(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit;

	public Future<Result<Void>> lockAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit;
	 
	public Future<Result<Void>> unlockAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit;
	 
	public Future<Result<byte[]>> getHiddenAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit;

	public Future<ResultMap<byte[], Result<Void>>> batchLockAsync(short ns, List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit;
	 
	public Future<ResultMap<byte[], Result<Void>>> batchUnlockAsync(short ns, List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit;
	 
	public Future<Result<Void>> prefixPutAsync(short ns, byte[] pkey, byte[] skey, byte[] value, TairOption opt) throws TairRpcError, TairFlowLimit;

	public Future<Result<byte[]>> prefixGetAsync(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<Void>> prefixSetCountAsync(short ns, byte[] pkey, byte[] skey, int count, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<byte[]>> prefixGetHiddenAsync(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<Integer>> prefixIncrAsync(short ns, byte[] pkey, byte[] skey, int value, int defaultValue, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<Integer>> prefixDecrAsync(short ns, byte[] pkey, byte[] skey, int value, int initValue, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<Integer>> prefixIncrAsync(short ns, byte[] pkey, byte[] skey, int value, int defaultValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<Integer>> prefixDecrAsync(short ns, byte[] pkey, byte[] skey, int value, int initValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	
	public Future<Result<Void>> hideByProxyAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException;
	
	public Future<Result<Void>> prefixHideByProxyAsync(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException;
	
	public Future<ResultMap<byte[], Result<Void>>> prefixHideMultiByProxyAsync(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException;
	
	public Future<Result<Void>> invalidByProxyAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException;
	
	public Future<Result<Void>> prefixInvalidByProxyAsync(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException;
	
	public Future<ResultMap<byte[], Result<Void>>> prefixInvalidMultiByProxyAsync(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException;
	
	public Future<ResultMap<byte[], Result<Void>>> batchInvalidByProxyAsync(short ns, final List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException;
	
	public Future<Result<Void>> expireAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<ResultMap<byte[], Result<Void>>> prefixPutMultiAsync(short ns, byte[] pkey, final Map<byte[], Pair<byte[], RequestOption>> kvs, TairOption opt)  throws TairRpcError, TairFlowLimit;
	
	public Future<ResultMap<byte[], Result<byte[]>>> prefixGetMultiAsync(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt)  throws TairRpcError, TairFlowLimit;
	
	public Future<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>> batchPrefixGetMultiAsync(short ns, Map<byte[], List<byte[]>> kvs, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<ResultMap<byte[], Result<byte[]>>> prefixGetHiddenMultiAsync(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt)  throws TairRpcError, TairFlowLimit;
	
	public Future<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>> batchPrefixGetHiddenMultiAsync(short ns, Map<byte[], List<byte[]>> kvs, TairOption opt)  throws TairRpcError, TairFlowLimit;
	
	public Future<ResultMap<byte[], Result<Integer>>> prefixIncrMultiAsync(short ns, byte[] pkey, Map<byte[], Counter> skv, TairOption opt)  throws TairRpcError, TairFlowLimit;
	
	public Future<ResultMap<byte[], Result<Integer>>> prefixDecrMultiAsync(short ns, byte[] pkey, Map<byte[], Counter> skv, TairOption opt)  throws TairRpcError, TairFlowLimit;
	
	public Future<Result<List<Pair<byte[], Result<byte[]>>>>> getRangeAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<List<Result<byte[]>>>> deleteRangeAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<List<Result<byte[]>>>> getRangeKeyAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public Future<Result<List<Result<byte[]>>>> getRangeValueAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws TairRpcError, TairFlowLimit;
	
	public class NotifyFuture{
		private Future<?> future;
		private Object    ctx;
		public Future<?> getFuture() {
			return future;
		}
		public void setFuture(Future<?> future) {
			this.future = future;
		}
		public Object getCtx() {
			return ctx;
		}
		public void setCtx(Object ctx) {
			this.ctx = ctx;
		}
		public NotifyFuture(Future<?> future, Object ctx) {
			super();
			this.future = future;
			this.ctx = ctx;
		}
	}
	
	public void notifyFuture(Future<?> future, Object ctx) throws TairQueueOverflow;
	
	public NotifyFuture poll(long timeout, TimeUnit unit) throws InterruptedException;
	
	public NotifyFuture poll() throws InterruptedException;
}
