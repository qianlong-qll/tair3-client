package com.taobao.tair3.client.impl;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.impl.cast.TairResultCastFactory;
import com.taobao.tair3.client.packets.configserver.QueryInfoRequest;
import com.taobao.tair3.client.packets.configserver.QueryInfoResponse;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairUtil;

public class DefaultTairClient extends AbstractTairClient {
	protected static final Logger log = LoggerFactory.getLogger(DefaultTairClient.class);
	
	public Result<byte[]> get(short ns, byte[] key, TairOption opt) 
			throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<byte[]>> future = getAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<byte[]> getHidden(short ns, byte[] key, TairOption opt) 
			throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<byte[]>> future = getHiddenAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Void> put(short ns, byte[] key, byte[] value, TairOption opt)
			throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = putAsync(ns, key, value, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Integer> incr(short ns, byte[] key, int value, int defaultValue, TairOption opt)
			throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		if (value < 0) {
			throw new IllegalArgumentException(TairConstant.ITEM_VALUE_NOT_AVAILABLE);
		}
		Future<Result<Integer>> future = incrAsync(ns, key, value, defaultValue, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<Integer> incr(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, TairOption opt)
			throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Integer>> future = incrAsync(ns, key, value, defaultValue, lowBound, upperBound, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Integer> decr(short ns, byte[] key, int value, int defaultValue, TairOption opt)
			throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		if (value < 0) {
			throw new IllegalArgumentException(TairConstant.ITEM_VALUE_NOT_AVAILABLE);
		}
		Future<Result<Integer>> future = decrAsync(ns, key, value, defaultValue, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Integer> decr(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, TairOption opt)
			throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Integer>> future = decrAsync(ns, key, value, defaultValue, lowBound, upperBound, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Void> lock(short ns, byte[] key, TairOption opt)
			throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = lockAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Void> unlock(short ns, byte[] key, TairOption opt)
			throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = unlockAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public ResultMap<byte[], Result<Void>> batchPut(short ns, Map<byte[], byte[]> kv, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException  {
		Future<ResultMap<byte[], Result<Void>>> futureSet = batchPutAsync(ns, kv, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public ResultMap<byte[], Result<byte[]>> batchGet(short ns, List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException  {
		Future<ResultMap<byte[], Result<byte[]>>> futureSet = batchGetAsync(ns, keys, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public ResultMap<byte[], Result<Void>> batchLock(short ns, List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException  {
		Future<ResultMap<byte[], Result<Void>>> futureSet = batchLockAsync(ns, keys, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	public ResultMap<byte[], Result<Void>> batchUnlock(short ns, List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException  {
		Future<ResultMap<byte[], Result<Void>>> futureSet = batchUnlockAsync(ns, keys, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	public Result<Void> prefixPut(short ns, byte[] pkey, byte[] skey, byte[] value, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = prefixPutAsync(ns, pkey, skey, value, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<byte[]> prefixGet(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<byte[]>> future = prefixGetAsync(ns, pkey, skey, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<byte[]> prefixGetHidden(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<byte[]>> future = prefixGetHiddenAsync(ns, pkey, skey, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public ResultMap<byte[], Result<Void>> prefixPutMulti(short ns, byte[] pkey, final Map<byte[], Pair<byte[], RequestOption>> kvs, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Void>>> future = prefixPutMultiAsync(ns, pkey, kvs, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public ResultMap<byte[], Result<Void>> prefixPutMulti(short ns, byte[] pkey, final Map<byte[], Pair<byte[], RequestOption>> kvs, final Map<byte[], Pair<Integer, RequestOption>> cvs, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Void>>> future = prefixPutMultiAsync(ns, pkey, kvs, cvs, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public ResultMap<byte[], Result<Void>> prefixSetCountMulti(short ns, byte[] pkey, final Map<byte[], Pair<Integer, RequestOption>> kvs, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Void>>> future = prefixSetCountMultiAsync(ns, pkey, kvs, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public ResultMap<byte[], Result<byte[]>> prefixGetMulti(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<byte[]>>> future = prefixGetMultiAsync(ns, pkey, skeys, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public ResultMap<byte[], Result<byte[]>> prefixGetHiddenMulti(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<byte[]>>> future = prefixGetHiddenMultiAsync(ns, pkey, skeys, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<Void> setCount(short ns, byte[] key, int count, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = this.setCountAsync(ns, key, count, opt);
		return futureGet(future,opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Void> prefixSetCount(short ns, byte[] pkey, byte[] skey, int count, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = this.prefixSetCountAsync(ns, pkey, skey, count, opt);
		return futureGet(future,opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}


	public Result<Integer> prefixIncr(short ns, byte[] pkey, byte[] skey, int value, int initValue, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Integer>> future = prefixIncrAsync(ns, pkey, skey, value, initValue, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	public Result<Integer> prefixIncr(short ns, byte[] pkey, byte[] skey, int value, int initValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Integer>> future = prefixIncrAsync(ns, pkey, skey, value, initValue, lowBound, upperBound, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<Integer> prefixDecr(short ns, byte[] pkey, byte[] skey, int value, int initValue, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Integer>> future = prefixDecrAsync(ns, pkey, skey, value, initValue, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	public Result<Integer> prefixDecr(short ns, byte[] pkey, byte[] skey, int value, int initValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Integer>> future = prefixDecrAsync(ns, pkey, skey, value, initValue, lowBound, upperBound, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public ResultMap<byte[], Result<Integer>> prefixIncrMulti(short ns, byte[] pkey, Map<byte[], Counter> skv, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Integer>>> futureSet = prefixIncrMultiAsync(ns, pkey, skv, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	public ResultMap<byte[], Result<Integer>> prefixIncrMulti(short ns, byte[] pkey, Map<byte[], Counter> skv, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Integer>>> futureSet = prefixIncrMultiAsync(ns, pkey, skv, lowBound, upperBound, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public ResultMap<byte[], Result<Integer>> prefixDecrMulti(short ns, byte[] pkey, Map<byte[], Counter> skv, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Integer>>> futureSet = prefixDecrMultiAsync(ns, pkey, skv, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	public ResultMap<byte[], Result<Integer>> prefixDecrMulti(short ns, byte[] pkey, Map<byte[], Counter> skv, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Integer>>> futureSet = prefixDecrMultiAsync(ns, pkey, skv, lowBound, upperBound, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>> batchPrefixGetMulti(short ns, Map<byte[], List<byte[]>> keys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>> futureSet = batchPrefixGetMultiAsync(ns, keys, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>> batchPrefixGetHiddenMulti(short ns, Map<byte[], List<byte[]>> keys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>> futureSet = batchPrefixGetHiddenMultiAsync(ns, keys, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<Void> invalidByProxy(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = invalidByProxyAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}

	public Result<Void> hideByProxy(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = hideByProxyAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	public Result<Void> prefixInvalidByProxy(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = prefixInvalidByProxyAsync(ns, pkey, skey, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public ResultMap<byte[], Result<Void>> prefixInvalidMultiByProxy(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Void>>> futureSet = prefixInvalidMultiByProxyAsync(ns, pkey, skeys, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<Void> prefixHideByProxy(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = prefixHideByProxyAsync(ns, pkey, skey, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public ResultMap<byte[], Result<Void>> prefixHideMultiByProxy(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Void>>> futureSet = prefixHideMultiByProxyAsync(ns, pkey, skeys, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public ResultMap<byte[], Result<Void>> batchInvalidByProxy(short ns, final List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Void>>> futureSet = batchInvalidByProxyAsync(ns, keys, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
		

	public Result<List<Pair<byte[], Result<byte[]>>>> getRange(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws InterruptedException, TairTimeout, TairRpcError, TairFlowLimit {
		Future<Result<List<Pair<byte[], Result<byte[]>>>>> futureSet = getRangeAsync(ns, pkey, begin, end, offset, maxCount, reverse, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}
	
	public Result<List<Result<byte[]>>> deleteRange(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws InterruptedException, TairTimeout, TairRpcError, TairFlowLimit {
		Future<Result<List<Result<byte[]>>>> future = deleteRangeAsync(ns, pkey, begin, end, offset, maxCount, reverse, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());		
	}


	public Result<List<Result<byte[]>>> getRangeKey(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws InterruptedException, TairTimeout, TairRpcError, TairFlowLimit {
		Future<Result<List<Result<byte[]>>>> future = getRangeKeyAsync(ns, pkey, begin, end, offset, maxCount, reverse, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}

	public Result<List<Result<byte[]>>> getRangeValue(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws InterruptedException, TairTimeout, TairRpcError, TairFlowLimit {
		Future<Result<List<Result<byte[]>>>> future = getRangeValueAsync(ns, pkey, begin, end, offset, maxCount, reverse, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}

	public Result<Void> expire(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = expireAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}

	public ResultMap<byte[], Result<byte[]>> simplePrefixGetMulti(
			short ns, byte[] pkey, List<byte[]> skeys, TairOption opt)
			throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<byte[]>>> futureSet = simplePrefixGetMultiAsync(ns, pkey, skeys, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	
	public Result<Map<String, String>> getStat(int qtype,
			String group, long serverId, TairOption opt) throws TairRpcError,
			TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Map<String, String>>> future = getStatAsync(qtype, group, serverId, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}

	private <T> T futureGet(Future<T> future, long timeout) throws InterruptedException, TairTimeout, TairRpcError {
		try {
			return future.get(timeout, TimeUnit.MILLISECONDS);
		} catch (ExecutionException e) {
			log.debug("exception: ", e);
			Throwable t = e.getCause();
			if (t instanceof TairTimeout) {
				throw (TairTimeout)t;
			} 
			if (t instanceof TairRpcError) {
				throw (TairRpcError)t;
			}
			throw new TairRpcError(t);
		} catch (TimeoutException e) {
			throw new TairTimeout(e);
		}
	}
}
