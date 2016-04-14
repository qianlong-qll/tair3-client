package com.taobao.tair3.client.impl;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.TairBlockingQueue;
import com.taobao.tair3.client.TairClient;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairQueueOverflow;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.impl.cast.TairResultCastFactory;
import com.taobao.tair3.client.packets.common.BatchReturnResponse;
import com.taobao.tair3.client.packets.common.ReturnResponse;
import com.taobao.tair3.client.packets.configserver.QueryInfoRequest;
import com.taobao.tair3.client.packets.configserver.QueryInfoResponse;
import com.taobao.tair3.client.packets.dataserver.BoundedIncDecRequest;
import com.taobao.tair3.client.packets.dataserver.BoundedPrefixIncDecRequest;
import com.taobao.tair3.client.packets.dataserver.DeleteRequest;
import com.taobao.tair3.client.packets.dataserver.ExpireRequest;
import com.taobao.tair3.client.packets.dataserver.GetHiddenRequest;
import com.taobao.tair3.client.packets.dataserver.GetRequest;
import com.taobao.tair3.client.packets.dataserver.GetResponse;
import com.taobao.tair3.client.packets.dataserver.HideRequest;
import com.taobao.tair3.client.packets.dataserver.IncDecRequest;
import com.taobao.tair3.client.packets.dataserver.IncDecResponse;
import com.taobao.tair3.client.packets.dataserver.LockRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixDeleteMultiRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixGetHiddenMultiRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixGetMultiRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixGetMultiResponse;
import com.taobao.tair3.client.packets.dataserver.PrefixHideMultiRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixIncDecRequest;
import com.taobao.tair3.client.packets.dataserver.PrefixIncDecResponse;
import com.taobao.tair3.client.packets.dataserver.PrefixPutMultiRequest;
import com.taobao.tair3.client.packets.dataserver.PutRequest;
import com.taobao.tair3.client.packets.dataserver.RangeRequest;
import com.taobao.tair3.client.packets.dataserver.RangeResponse;
import com.taobao.tair3.client.packets.dataserver.SimplePrefixGetMultiRequest;
import com.taobao.tair3.client.packets.dataserver.SimplePrefixGetMultiResponse;
import com.taobao.tair3.client.packets.invalidserver.HideByProxyMultiRequest;
import com.taobao.tair3.client.packets.invalidserver.HideByProxyRequest;
import com.taobao.tair3.client.packets.invalidserver.InvalidByProxyMultiRequest;
import com.taobao.tair3.client.packets.invalidserver.InvalidByProxyRequest;
import com.taobao.tair3.client.rpc.future.TairResultFuture;
import com.taobao.tair3.client.rpc.future.TairResultFutureImpl;
import com.taobao.tair3.client.rpc.future.TairResultFutureSetImpl;
import com.taobao.tair3.client.rpc.net.DeamondThreadFactory;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairUtil;


public abstract class AbstractTairClient implements TairClient {
	private static Logger logger = LoggerFactory.getLogger(AbstractTairClient.class);
	public  TairOption defaultOptions = new TairOption(500);
	private TairProcessor tairProcessor = null;
	private String master;
	private String slave;
	private String group;
	
	private static int workerThreadCount = Runtime.getRuntime().availableProcessors() / 4 + 1;
	private static int bossThreadCount = (Runtime.getRuntime().availableProcessors() + 7) / 8;
	private static String workerThreadCountKey = "tair.nio.workercount";
    private static ExecutorService bossThreadPool = null;
    private static ExecutorService workerThreadPool = null;
    private static void initThreadCount() {
    	String workerThreadCountStr = System.getProperty(workerThreadCountKey);
    	if (workerThreadCountStr != null) {
    		try {
    			workerThreadCount = Integer.parseInt(workerThreadCountStr);
    			logger.info("worker thread from the system property: " + workerThreadCountStr);	 
    		} catch (NumberFormatException e) {
    			logger.error("failed to get the worker thread from the system property: " + workerThreadCountStr, e);	 
    		}
    	}
    }
    static {
    	initThreadCount();
    	bossThreadPool = Executors.newCachedThreadPool(new DeamondThreadFactory("tair-boss-share"));
    	workerThreadPool = Executors.newCachedThreadPool(new DeamondThreadFactory("tair-worker-share"));
    }
	
	private static NioClientSocketChannelFactory defaultNioFactory = new NioClientSocketChannelFactory(bossThreadPool, workerThreadPool, bossThreadCount, workerThreadCount);
	private NioClientSocketChannelFactory nioFactory = defaultNioFactory;
	
	
	private int maxNotifyQueueSize = 512;
	
	private static TairBlockingQueue notifyQueue = new DefaultTairBlockingQueue(); 
	
	public AbstractTairClient() {
		defaultOptions.setVersion ((short)0);
		defaultOptions.setExpireTime(0);
		defaultOptions.setTimeout(500);
	}
	
	public void setTimeout(int timeout) {
		defaultOptions.setTimeout(timeout);
	}
	
	public void setMaxNotifyQueueSize(int maxNotifyQueueSize) {
		this.maxNotifyQueueSize = maxNotifyQueueSize;
	}
	
	public TairBlockingQueue getNotifyQueue() {
		return notifyQueue;
	}

	public void setNotifyQueue(TairBlockingQueue notifyQueueNew) {
		notifyQueue.clear();
		notifyQueue = notifyQueueNew;
	}

	public int getMaxNotifyQueueSize() {
		return maxNotifyQueueSize;
	}

	public void setMaster(String master) {
		this.master = master;
	}
	public String getMaster() {
		return master;
	}

	public void setSlave(String slave) {
		this.slave = slave;
	}
	public String getSlave() {
		return slave;
	}

	public void setWorkerThreadCount(int count) {
		workerThreadCount = count;
	}
	public int getWorkerThreadCount() {
		return workerThreadCount;
	}

	public static void setBossThreadCount(int count) {
		bossThreadCount = count;
	}
	
	public int getBossThreadCount() {
		return bossThreadCount;
	}

	public void setGroup(String group) {
		//if (group.endsWith("\0"))
			this.group = group;
		//else
		//	this.group = group + "\0";
	}
	
	public String getGroup() {
		return this.group;
	}

	public NioClientSocketChannelFactory getNioFactory() {
		return nioFactory;
	}

	public void setNioFactory(NioClientSocketChannelFactory nioFactory) {
		this.nioFactory = nioFactory;
	}
	
	public void setTairProcessor(TairProcessor tairProcessor) {
		this.tairProcessor = tairProcessor;
	}
	public void init() throws TairException {
		//if (tairProcessor != null)
	    //		throw new TairException("had inited");
		//tairProcessor = new TairProcessor(master, slave, group, nioFactory);
		if (tairProcessor == null) {
			String groupName = group;
			if (!groupName.endsWith("\0"))  {
				groupName = group + "\0";
			}
			tairProcessor = new TairProcessor(master, slave, groupName, nioFactory);
		}
		tairProcessor.init();
		logger.info("Tair3 Client start, connect to : " + this.group);
	}
	
	public void close() {
		if (nioFactory != defaultNioFactory) {
			nioFactory.shutdown();
		}
	}
	
	public static void shutdown() {
		TairProcessor.shutdown();
		defaultNioFactory.releaseExternalResources();
		defaultNioFactory.shutdown();
	}

	private Future<Result<Void>> putAsyncImpl(short ns, byte[] pkey, byte[] skey, int keyFlag, byte[] value, int valueFlag, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) {
			opt = defaultOptions;
		}
		//OK!!!
		PutRequest request = PutRequest.build(ns, pkey, skey, keyFlag, value, valueFlag, opt);
		request.setContext((short)(skey != null ? pkey.length : 0));
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.PUT);
	}
	
	
	public Future<Result<Void>> putAsync(short ns, byte[] key, byte[] value, TairOption opt) throws TairRpcError, TairFlowLimit {
		//why ?
		int keyFlag = (opt != null && opt.getRequestOption() != null && opt.getRequestOption().getVersion() != 0)  ? 1 : 0;
		return putAsyncImpl(ns, key, null, keyFlag, value, 0, opt);
	}
	
	private Future<Result<byte[]>> getAsyncImpl(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null)
			opt = defaultOptions;
		//OK!!!
		GetRequest request = GetRequest.build(ns, pkey, skey);
		request.setContext((short)(skey != null ? pkey.length : 0));
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), GetResponse.class, TairResultCastFactory.GET);
	}

	public Future<Result<byte[]>> getAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit {
		return getAsyncImpl(ns, key, null, opt);
	}
	
	private Future<Result<Void>> deleteLocalAsyncImpl(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//OK!!!
		DeleteRequest request = DeleteRequest.build(ns, pkey, skey);
		request.setContext((short)(skey != null ? pkey.length : 0));
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.DELETE);
	}

	public Future<ResultMap<byte[], Result<Void>>> batchPutAsync(short ns, final Map<byte[], byte[]> kv, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null)
			opt = defaultOptions;
		if (kv == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		Set<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>> futureSet = new HashSet<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>>();
		//send the request.
		for (Map.Entry<byte[], byte[]> entry : kv.entrySet()) {
			byte[] key = entry.getKey();
			byte[] value = entry.getValue();
			SocketAddress addr = tairProcessor.matchDataServer(key);
			//OK!!!
			PutRequest request = PutRequest.build(ns, key, null, 0, value, 0, opt);
			request.setContext(key);
			TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.BATCH_PUT_OLD);
			futureSet.add(future);
		}
		//return the future set.
		return new TairResultFutureSetImpl<ReturnResponse, Void, ResultMap<byte[], Result<Void>>>(futureSet);
	}
	
	public Future<ResultMap<byte[], Result<byte[]>>> batchGetAsync(short ns, final List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		if (keys == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		Map<SocketAddress, List<byte[]>> batch = tairProcessor.matchDataServer(keys);

		Set<TairResultFutureImpl<GetResponse, Result<ResultMap<byte[], Result<byte[]>>>>> futureSet = new HashSet<TairResultFutureImpl<GetResponse, Result<ResultMap<byte[], Result<byte[]>>>>>();
		//send the request
		for (SocketAddress addr : batch.keySet()) {
			GetRequest request =  GetRequest.build(ns, batch.get(addr), opt);
			request.setContext(batch.get(addr));
			TairResultFutureImpl<GetResponse, Result<ResultMap<byte[], Result<byte[]>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), GetResponse.class, TairResultCastFactory.BATCH_GET);
			//add the future to future set.
			futureSet.add(future);
		}
		//return the future set.
		return new TairResultFutureSetImpl<GetResponse, byte[], ResultMap<byte[], Result<byte[]>>>(futureSet);
	}

	private Future<Result<Integer>> addCountAsyncImpl(short ns, byte[] pkey, byte[] skey, int value, int defaultValue, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//OK!!!
		IncDecRequest request = IncDecRequest.build(ns, pkey, skey, value, defaultValue, opt);
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), IncDecResponse.class, TairResultCastFactory.ADD_COUNT);
	}
	
	private Future<Result<Integer>> addCountBoundedAsyncImpl(short ns, byte[] pkey, byte[] skey, int value, int defaultValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//OK!!!
		BoundedIncDecRequest request = BoundedIncDecRequest.build(ns, pkey, skey, value, defaultValue, lowBound, upperBound, opt);
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), IncDecResponse.class, TairResultCastFactory.ADD_COUNT_BOUNDED);
	}
	
	public Future<Result<Void>> setCountAsync(short ns, byte[] key, int count, TairOption opt) throws TairRpcError, TairFlowLimit {
		byte[] incValue = TairUtil.encodeCountValue(count);
		return putAsyncImpl(ns, key, null, 0, incValue, TairConstant.TAIR_ITEM_FLAG_ADDCOUNT, opt);
	}

	public Future<Result<Integer>> incrAsync(short ns, byte[] key, int value, int defaultValue, TairOption opt) throws TairRpcError, TairFlowLimit {
		return addCountAsyncImpl(ns, key, null, value, defaultValue, opt);
	}
	public Future<Result<Integer>> incrAsync(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit {
		return addCountBoundedAsyncImpl(ns, key, null, value, defaultValue, lowBound, upperBound, opt);
	}
	
	public Future<Result<Integer>> decrAsync(short ns, byte[] key, int value, int defaultValue, TairOption opt) throws TairRpcError, TairFlowLimit {
		return addCountAsyncImpl(ns, key, null, -value, defaultValue, opt);
	}
	public Future<Result<Integer>> decrAsync(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit {
		return addCountBoundedAsyncImpl(ns, key, null, -value, defaultValue, lowBound, upperBound, opt);
	}
	
	private Future<Result<Void>> lockKeyAsync(short ns, byte[] key, int lockType, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//OK!!!
		LockRequest request = LockRequest.build(ns, key, lockType);
		SocketAddress addr = tairProcessor.matchDataServer(key);
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.LOCK_KEY);
	}
	
	public Future<Result<Void>> lockAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit {
		return lockKeyAsync(ns, key, LockRequest.LOCK_VALUE, opt);
	}
	
	public Future<Result<Void>> unlockAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit {
		return lockKeyAsync(ns, key, LockRequest.UNLOCK_VALUE, opt);
	}
	
	private Future<Result<byte[]>> getHiddenAsyncImpl(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit { 
		if (opt == null) 
			opt = defaultOptions;
		GetHiddenRequest request = GetHiddenRequest.build(ns, pkey, skey);
		request.setContext((short)(skey != null ? pkey.length : 0));
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), GetResponse.class, TairResultCastFactory.GET_HIDDEN);
	}

	public Future<Result<byte[]>> getHiddenAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit { 
		return getHiddenAsyncImpl(ns, key, null, opt);
	}

	private Future<Result<Void>> hideLocalAsyncImpl(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		HideRequest request = HideRequest.build(ns, pkey, skey);
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.HIDE);
	}

	private Future<ResultMap<byte[], Result<Void>>> batchLockKeyAsync(short ns, List<byte[]> keys, int lockType, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null)
			opt = defaultOptions;
		if (keys == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		Set<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>> futureSet = new HashSet<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>>();
		// send the request.
		for (byte[] key : keys) {
			SocketAddress addr = tairProcessor.matchDataServer(key);
			//OK!!!
			LockRequest request = LockRequest.build(ns, key, lockType);
			request.setContext(key);
			TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.BATCH_LOCK_KEY);
			//add future
			futureSet.add(future);
		}
		//return the future set.
		return new TairResultFutureSetImpl<ReturnResponse, Void, ResultMap<byte[], Result<Void>>>(futureSet);
	}

	public Future<ResultMap<byte[], Result<Void>>> batchLockAsync(short ns, List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit {
		return batchLockKeyAsync(ns, keys, LockRequest.LOCK_VALUE, opt);
	}

	public Future<ResultMap<byte[], Result<Void>>> batchUnlockAsync(short ns, List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit {
		return batchLockKeyAsync(ns, keys, LockRequest.UNLOCK_VALUE, opt);
	}

	public Future<Result<Void>> prefixPutAsync(short ns, byte[] pkey, byte[] skey, byte[] value, TairOption opt) throws TairRpcError, TairFlowLimit {
		return putAsyncImpl(ns, pkey, skey, 0, value, 0, opt);
	}
	
	private Future<ResultMap<byte[], Result<Void>>> prefixDeleteMultiLocalAsyncImpl(short ns, byte[] pkey, final List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//OK!!!
		PrefixDeleteMultiRequest request = PrefixDeleteMultiRequest.build(ns, pkey, skeys);
		Pair<byte[], List<byte[]>> context = new Pair<byte[], List<byte[]>> (pkey, skeys);
		request.setContext(context);

		//send request
		SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		TairResultFutureImpl<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), BatchReturnResponse.class, TairResultCastFactory.PREFIX_DELETE_MULTI);
		//add the future the the set.
		Set<TairResultFutureImpl<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>>> futureSet = new HashSet<TairResultFutureImpl<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>>>();
		futureSet.add(future);
		//create the futureSet.
		return new TairResultFutureSetImpl<BatchReturnResponse, Void, ResultMap<byte[], Result<Void>>>(futureSet);
	}
	
	public Future<Result<byte[]>> prefixGetAsync(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		if (skey == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		return getAsyncImpl(ns, pkey, skey, opt);
	}
	
	public Future<Result<Void>> prefixSetCountAsync(short ns, byte[] pkey, byte[] skey, int count, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (skey == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		byte[] incValue = TairUtil.encodeCountValue(count);
		return putAsyncImpl(ns, pkey, skey, 0, incValue, TairConstant.TAIR_ITEM_FLAG_ADDCOUNT, opt);
	}
	
	public Future<Result<byte[]>> prefixGetHiddenAsync(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (skey == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		return getHiddenAsyncImpl(ns, pkey, skey, opt);
	}

	public Future<Result<Integer>> prefixIncrAsync(short ns, byte[] pkey, byte[] skey, int count, int defaultValue, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (skey == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		return addCountAsyncImpl(ns, pkey, skey, count, defaultValue, opt);
	}
	public Future<Result<Integer>> prefixIncrAsync(short ns, byte[] pkey, byte[] skey, int count, int defaultValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (skey == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		return addCountBoundedAsyncImpl(ns, pkey, skey, count, defaultValue, lowBound, upperBound, opt);
	}
	
	public Future<Result<Integer>> prefixDecrAsync(short ns, byte[] pkey, byte[] skey, int count, int defaultValue, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (skey == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		return addCountAsyncImpl(ns, pkey, skey, -count, defaultValue, opt);
	}
	public Future<Result<Integer>> prefixDecrAsync(short ns, byte[] pkey, byte[] skey, int count, int defaultValue, int lowBound, int upperBound, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (skey == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		return addCountBoundedAsyncImpl(ns, pkey, skey, -count, defaultValue, lowBound, upperBound, opt);
	}
	
	private Future<Result<Void>> hideByProxyAsyncImpl(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		if (opt == null) {
			opt = defaultOptions;
		}
		HideByProxyRequest request = HideByProxyRequest.build(ns, pkey, skey, group);
		request.setContext((short)(skey != null ? pkey.length : 0));
		return tairProcessor.callInvalidServerAsync(request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.HIDE_BY_PROXY);
	}
	
	public Future<Result<Void>> hideByProxyAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = hideByProxyAsyncImpl(ns, key, null, opt);
		if (future == null)
			return hideLocalAsyncImpl(ns, key, null, opt);
		return future;
	}
	
	public Future<Result<Void>> prefixHideByProxyAsync(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		if (skey == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		Future<Result<Void>> future = hideByProxyAsyncImpl(ns, pkey, skey, opt);
		if (future == null)
			return hideLocalAsyncImpl(ns, pkey, skey, opt);
		return future;
	}

	public Future<ResultMap<byte[], Result<Void>>> prefixHideMultiByProxyAsync(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Void>>> future = prefixHideMultiByProxyAsyncImpl(ns, pkey, skeys, opt);
		if (future == null)
			return prefixHideMultiLocalAsyncImpl(ns, pkey, skeys, opt); 
		return future;
	}
	
	private Future<ResultMap<byte[], Result<Void>>> prefixHideMultiByProxyAsyncImpl(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		if (opt == null) {
			opt = defaultOptions;
		}
		//build request
		//OK!!!
		List<byte[]> skeys_ = TairUtil.removeDuplicateKeys(skeys);
		HideByProxyMultiRequest request = HideByProxyMultiRequest.build(ns, pkey, skeys_, group);

		Pair<byte[], List<byte[]>> context = new Pair<byte[], List<byte[]>> (pkey, skeys_);
		request.setContext(context);
		//send request
		TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> future = tairProcessor.callInvalidServerAsync(request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.PREFIX_HIDE_MULTI_BY_PROXY);
		//add the future the the set.
		if (future == null)
			return null;
		Set<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>> futureSet = new HashSet<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>>();
		futureSet.add(future);
		//create the futureSet.
		return new TairResultFutureSetImpl<ReturnResponse, Void, ResultMap<byte[], Result<Void>>>(futureSet);
	}
	
	private Future<Result<Void>> deleteByProxyAsyncImpl(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		if (opt == null) {
			opt = defaultOptions;
		}
		//OK!!!
		InvalidByProxyRequest request = InvalidByProxyRequest.build(ns, pkey, skey, group);
		request.setContext((short)(skey != null ? pkey.length : 0));
		return tairProcessor.callInvalidServerAsync(request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.INVALID);	 
	}
	public Future<Result<Void>> invalidByProxyAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		return invalidByProxyAsyncImpl(ns, key, null, opt);
	}
	
	private Future<Result<Void>> invalidByProxyAsyncImpl(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<Result<Void>> future = deleteByProxyAsyncImpl(ns, pkey, skey, opt);
		if (future == null) 
			return deleteLocalAsyncImpl(ns, pkey, skey, opt);
		return future;
	}
	
	public Future<Result<Void>> prefixInvalidByProxyAsync(short ns, byte[] pkey, byte[] skey, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		if (skey == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		return invalidByProxyAsyncImpl(ns, pkey, skey, opt);
	}

	public Future<ResultMap<byte[], Result<Void>>> prefixInvalidMultiByProxyAsync(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Void>>> future = prefixInvalidMultiByProxyAsyncImpl(
				ns, pkey, skeys, opt);
		if (future == null)
			return prefixDeleteMultiLocalAsyncImpl(ns, pkey, skeys, opt);
		return future;
	
	}

	private Future<ResultMap<byte[], Result<Void>>> prefixInvalidMultiByProxyAsyncImpl(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		if (opt == null) {
			opt = defaultOptions;
		}
		//build request
		//OK!!!
		List<byte[]> skeys_ = TairUtil.removeDuplicateKeys(skeys);
		InvalidByProxyMultiRequest request = InvalidByProxyMultiRequest.build(ns, pkey, skeys_, group);
		Pair<byte[], List<byte[]>> context = new Pair<byte[], List<byte[]>>(pkey, skeys_);
		request.setContext(context);
		//send request
		TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> future = tairProcessor.callInvalidServerAsync(request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.PREFIX_INVALID_MULTI);
		if (future == null)
			return null;
		//add the future the the set.
		Set<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>> futureSet = new HashSet<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>>();
		futureSet.add(future);
		//create the futureSet.
		return new TairResultFutureSetImpl<ReturnResponse, Void, ResultMap<byte[], Result<Void>>>(futureSet);
	}
	
	public Future<ResultMap<byte[], Result<Void>>> batchInvalidByProxyAsync(short ns, final List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<Void>>> future = batchDeleteByProxyAsync(
				ns, keys, opt);
		if (future == null)
			return batchDeleteLocalAsync(ns, keys, opt);
		return future;
	}

	private Future<ResultMap<byte[], Result<Void>>> batchDeleteByProxyAsync(short ns, final List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
		if (opt == null) {
			opt = defaultOptions;
		}
		if (keys == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		//build request.
		//OK!!!
		List<byte[]> keys_ = TairUtil.removeDuplicateKeys(keys);
		InvalidByProxyRequest request = InvalidByProxyRequest.build(ns, keys_, group);
		request.setContext(keys_);
		TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> future = tairProcessor.callInvalidServerAsync(request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.BATCH_INVALID);
		//add the future the the set.
		if (future == null)
			return null;
		Set<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>> futureSet = new HashSet<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>>();
		futureSet.add(future);
		//create the futureSet.
		return new TairResultFutureSetImpl<ReturnResponse, Void, ResultMap<byte[], Result<Void>>>(futureSet);
	
	}
	
	private Future<ResultMap<byte[], Result<Void>>> batchDeleteLocalAsync(short ns, final List<byte[]> keys, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		if (keys == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		Map<SocketAddress, List<byte[]>> batch = tairProcessor.matchDataServer(keys);

		Set<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>> futureSet = new HashSet<TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>>>();
		//send the request
		for (SocketAddress addr : batch.keySet()) {
			//OK!!!
			DeleteRequest request = DeleteRequest.build(ns, batch.get(addr));
			request.setContext(batch.get(addr));
			TairResultFutureImpl<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.BATCH_DELETE);
			//add the future to future set.
			futureSet.add(future);
		}
		//return the future set.
		return new TairResultFutureSetImpl<ReturnResponse, Void, ResultMap<byte[], Result<Void>>>(futureSet);
	}

	public Future<Result<Void>> expireAsync(short ns, byte[] key, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//build request.
		//OK!!!
		ExpireRequest request = ExpireRequest.build(ns, key, opt);
		SocketAddress addr = tairProcessor.matchDataServer(key);
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, TairResultCastFactory.EXPIRE);
	}

	private Future<ResultMap<byte[], Result<Void>>> prefixPutMultiAsyncImpl(short ns, byte[] pkey, final Map<byte[], Pair<byte[], RequestOption>> kvs, final Map<byte[], Pair<byte[], RequestOption>> cvs, TairOption opt)  throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		
		PrefixPutMultiRequest request = PrefixPutMultiRequest.build(ns, pkey, kvs, cvs);
		
		List<byte[]> keys = new ArrayList<byte[]> ();
		if (kvs != null) {
			List<byte[]> kvKeys = TairUtil.fetchRowKey(kvs);
			keys.addAll(kvKeys);
		}
		if (cvs != null) {
			List<byte[]> cvKeys = TairUtil.fetchRowKey(cvs);
			keys.addAll(cvKeys);
		} 
		//create request packet
		//OK!!!
		
		Pair<byte[], List<byte[]>> context = new Pair<byte[], List<byte[]>>(pkey, keys);
		request.setContext(context);
		//send request
		SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		TairResultFutureImpl<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), BatchReturnResponse.class, TairResultCastFactory.PREFIX_PUT_MULTI);
		//add the future the the set.
		Set<TairResultFutureImpl<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>>> futureSet = new HashSet<TairResultFutureImpl<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>>>();
		futureSet.add(future);
		//create the futureSet.
		return new TairResultFutureSetImpl<BatchReturnResponse, Void, ResultMap<byte[], Result<Void>>>(futureSet);
	}
	
	public Future<ResultMap<byte[], Result<Void>>> prefixPutMultiAsync(short ns, byte[] pkey, final Map<byte[], Pair<byte[], RequestOption>> kvs, TairOption opt)  throws TairRpcError, TairFlowLimit {
		return prefixPutMultiAsyncImpl(ns, pkey, kvs, null, opt);
	}
	
	public Future<ResultMap<byte[], Result<Void>>> prefixPutMultiAsync(short ns, byte[] pkey, final Map<byte[], Pair<byte[], RequestOption>> kvs, final Map<byte[], Pair<Integer, RequestOption>> cvs, TairOption opt)  throws TairRpcError, TairFlowLimit {
		Map<byte[], Pair<byte[], RequestOption>> cvsTemp = null;
		if (cvs != null) {
			cvsTemp = new HashMap<byte[], Pair<byte[], RequestOption>>(
					cvs.size());
			for (Map.Entry<byte[], Pair<Integer, RequestOption>> entry : cvs.entrySet()) {
				byte[] incValue = TairUtil.encodeCountValue(entry.getValue().first());
				cvsTemp.put(entry.getKey(), new Pair<byte[], RequestOption>(
						incValue, entry.getValue().second()));
			}
		}
		return prefixPutMultiAsyncImpl(ns, pkey, kvs, cvsTemp, opt);
	}
		
	
	public Future<ResultMap<byte[], Result<Void>>> prefixSetCountMultiAsync(short ns, byte[] pkey, final Map<byte[], Pair<Integer, RequestOption>> kvs, TairOption opt)  throws TairRpcError, TairFlowLimit {
		Map<byte[], Pair<byte[], RequestOption>> cvs = new HashMap<byte[], Pair<byte[], RequestOption>> (kvs.size());
		for (Map.Entry<byte[], Pair<Integer, RequestOption>> entry : kvs.entrySet()) {
			byte[] incValue = TairUtil.encodeCountValue(entry.getValue().first());
			cvs.put(entry.getKey(), new Pair<byte[], RequestOption> (incValue, entry.getValue().second()));
		}
		return prefixPutMultiAsyncImpl(ns, pkey, null, cvs, opt);
	}
	
	public Future<ResultMap<byte[], Result<byte[]>>> prefixGetMultiAsync(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt)  throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//build request
		//OK!!!
		PrefixGetMultiRequest request = PrefixGetMultiRequest.build(ns, pkey, skeys);
		request.setContext(skeys.size());
		SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), PrefixGetMultiResponse.class, TairResultCastFactory.PREFIX_GET_MULTI);
		//add the future the the set.
		Set<TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>>> futureSet = new HashSet<TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>>>();
		futureSet.add(future);
		//create the futureSet.
		return new TairResultFutureSetImpl<PrefixGetMultiResponse, byte[], ResultMap<byte[], Result<byte[]>>>(futureSet);
	}
		
	public Future<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>> batchPrefixGetMultiAsync(short ns, Map<byte[], List<byte[]>> kvs, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//create futureSet.
		Set<TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>>> futureSet = new HashSet<TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>>>();
		for (Map.Entry<byte[], List<byte[]>> e : kvs.entrySet()) {
			byte[] pkey = e.getKey();
			List<byte[]> skeys = e.getValue();
			//final List<ByteArray> skeySet = TairUtil.convertList(skeys);
			//create request packet.
			//OK!!!
			PrefixGetMultiRequest request = PrefixGetMultiRequest.build(ns, pkey, skeys);
			//send request.
			SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
			TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), PrefixGetMultiResponse.class, TairResultCastFactory.BATCH_PREFIX_GET_MULTI);
			//add the future the the set.
			futureSet.add(future);
		}
		//create the futureSet.
		return new TairResultFutureSetImpl<PrefixGetMultiResponse, Map<byte[], Result<byte[]>>, ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>(futureSet);
	}
	
	private Future<ResultMap<byte[], Result<Void>>> prefixHideMultiLocalAsyncImpl(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt)  throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//build request
		//OK!!!
		PrefixHideMultiRequest request = PrefixHideMultiRequest.build(ns, pkey, skeys);
		Pair<byte[], List<byte[]>> context = new Pair<byte[], List<byte[]>>(pkey, skeys);
		request.setContext(context);
		SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		TairResultFutureImpl<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), BatchReturnResponse.class, TairResultCastFactory.PREFIX_HIDE_MULTI);
		//add the future the the set.
		Set<TairResultFutureImpl<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>>> futureSet = new HashSet<TairResultFutureImpl<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>>>();
		futureSet.add(future);
		//create the futureSet.
		return new TairResultFutureSetImpl<BatchReturnResponse, Void, ResultMap<byte[], Result<Void>>>(futureSet);
	}
	
	public Future<ResultMap<byte[], Result<byte[]>>> prefixGetHiddenMultiAsync(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt)  throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		 
		//build request, no need to set the context
		//OK!!!
		PrefixGetHiddenMultiRequest request = PrefixGetHiddenMultiRequest.build(ns, pkey, skeys);
		SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), PrefixGetMultiResponse.class, TairResultCastFactory.PREFIX_GET_HIDDEN_MULTI);
		//add the future the the set.
		Set<TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>>> futureSet = new HashSet<TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>>>();
		futureSet.add(future);
		//create the futureSet.
		return new TairResultFutureSetImpl<PrefixGetMultiResponse, byte[], ResultMap<byte[], Result<byte[]>>>(futureSet);
	}
	
	public Future<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>  batchPrefixGetHiddenMultiAsync(short ns, Map<byte[], List<byte[]>> kvs, TairOption opt)  throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		if (kvs == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		Set<TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>>> futureSet = new HashSet<TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>>>();
		for (Map.Entry<byte[], List<byte[]>> e : kvs.entrySet()) {
			byte[] pkey = e.getKey();
			List<byte[]> skeys = e.getValue();
			
			//build request
			//OK!!!
			PrefixGetHiddenMultiRequest request = PrefixGetHiddenMultiRequest.build(ns, pkey, skeys);
			
			SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
			TairResultFutureImpl<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), PrefixGetMultiResponse.class, TairResultCastFactory.BATCH_PREFIX_GET_HIDDEN_MULTI);
			//add the future the the set.
			futureSet.add(future);
		}
		//create the futureSet.
		return new TairResultFutureSetImpl<PrefixGetMultiResponse, Map<byte[], Result<byte[]>>, ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>(futureSet);

	}
	
	private Future<ResultMap<byte[], Result<Integer>>> prefixAddCountMultiAsync(short ns, byte[] pkey, Map<byte[], Counter> skv, TairOption opt)  throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//build request, no need to set the context.
		//OK!!!
		PrefixIncDecRequest request = PrefixIncDecRequest.build(ns, pkey, skv);
		List<ByteArray> skeySet = TairUtil.fetchByteArrayKey(skv);
		Pair<byte[], List<ByteArray>> context = new Pair<byte[], List<ByteArray>> (pkey, skeySet);
		request.setContext(context);

		SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		TairResultFutureImpl<PrefixIncDecResponse, Result<ResultMap<byte[], Result<Integer>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), PrefixIncDecResponse.class, TairResultCastFactory.PREFIX_ADD_COUNT_MULTI);
		//add the future the the set.
		Set<TairResultFutureImpl<PrefixIncDecResponse, Result<ResultMap<byte[], Result<Integer>>>>> futureSet = new HashSet<TairResultFutureImpl<PrefixIncDecResponse, Result<ResultMap<byte[], Result<Integer>>>>>();
		futureSet.add(future);
		//create the futureSet.
		return new TairResultFutureSetImpl<PrefixIncDecResponse, Integer, ResultMap<byte[], Result<Integer>>>(futureSet);
	}
	private Future<ResultMap<byte[], Result<Integer>>> prefixAddCountBoundedMultiAsync(short ns, byte[] pkey, Map<byte[], Counter> skv, int lowBound, int upperBound, TairOption opt)  throws TairRpcError, TairFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//build request, no need to set the context.
		//OK!!!
		BoundedPrefixIncDecRequest request = BoundedPrefixIncDecRequest.build(ns, pkey, skv, lowBound, upperBound);
		List<ByteArray> skeySet = TairUtil.fetchByteArrayKey(skv);
		Pair<byte[], List<ByteArray>> context = new Pair<byte[], List<ByteArray>> (pkey, skeySet);
		request.setContext(context);

		SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		TairResultFutureImpl<PrefixIncDecResponse, Result<ResultMap<byte[], Result<Integer>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), PrefixIncDecResponse.class, TairResultCastFactory.PREFIX_ADD_COUNT_BOUNDED_MULTI);
		//add the future the the set.
		Set<TairResultFutureImpl<PrefixIncDecResponse, Result<ResultMap<byte[], Result<Integer>>>>> futureSet = new HashSet<TairResultFutureImpl<PrefixIncDecResponse, Result<ResultMap<byte[], Result<Integer>>>>>();
		futureSet.add(future);
		//create the futureSet.
		return new TairResultFutureSetImpl<PrefixIncDecResponse, Integer, ResultMap<byte[], Result<Integer>>>(futureSet);
	}
	public Future<ResultMap<byte[], Result<Integer>>> prefixIncrMultiAsync(short ns, byte[] pkey, Map<byte[], Counter> skv, TairOption opt)  throws TairRpcError, TairFlowLimit {
		return prefixAddCountMultiAsync(ns, pkey, skv, opt);
	}
	public Future<ResultMap<byte[], Result<Integer>>> prefixIncrMultiAsync(short ns, byte[] pkey, Map<byte[], Counter> skv, int lowBound, int upperBound, TairOption opt)  throws TairRpcError, TairFlowLimit {
		return prefixAddCountBoundedMultiAsync(ns, pkey, skv, lowBound, upperBound, opt);
	}
	public Future<ResultMap<byte[], Result<Integer>>> prefixDecrMultiAsync(short ns, byte[] pkey, Map<byte[], Counter> skv, TairOption opt)  throws TairRpcError, TairFlowLimit {
		Map<byte[], Counter> skvTemp = new HashMap<byte[], Counter>();
		for (Map.Entry<byte[], Counter> e : skv.entrySet()) {
			skvTemp.put(e.getKey(), new Counter(-e.getValue().getValue(), e.getValue().getInitValue(), e.getValue().getExpire()));
		}
		return prefixAddCountMultiAsync(ns, pkey, skvTemp, opt);
	}
	public Future<ResultMap<byte[], Result<Integer>>> prefixDecrMultiAsync(short ns, byte[] pkey, Map<byte[], Counter> skv, int lowBound, int upperBound, TairOption opt)  throws TairRpcError, TairFlowLimit {
		Map<byte[], Counter> skvTemp = new HashMap<byte[], Counter>();
		for (Map.Entry<byte[], Counter> e : skv.entrySet()) {
			skvTemp.put(e.getKey(), new Counter(-e.getValue().getValue(), e.getValue().getInitValue(), e.getValue().getExpire()));
		}
		return prefixAddCountBoundedMultiAsync(ns, pkey, skvTemp, lowBound, upperBound, opt);
	}
	
	public Future<Result<List<Pair<byte[], Result<byte[]>>>>> getRangeAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null)
			opt = defaultOptions;
		RangeRequest request = RangeRequest.build(ns, pkey, begin, end, offset, maxCount, (reverse ? TairConstant.RANGE_ALL_REVERSE : TairConstant.RANGE_ALL), opt);
		request.setContext(pkey);
		SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), RangeResponse.class, TairResultCastFactory.GET_RANGE);
	}

	public Future<Result<List<Result<byte[]>>>> deleteRangeAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws TairRpcError, TairFlowLimit {
		return operateRangeKeyOrValueAsyncImpl(ns, pkey, begin, end, offset, maxCount, (reverse ? TairConstant.RANGE_DEL_REVERSE : TairConstant.RANGE_DEL), opt);
	}

	private Future<Result<List<Result<byte[]>>>> operateRangeKeyOrValueAsyncImpl(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, short type, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null)
			opt = defaultOptions;
		RangeRequest request = RangeRequest.build(ns, pkey, begin, end, offset, maxCount, type, opt);
		request.setContext(pkey);
		SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), RangeResponse.class, TairResultCastFactory.GET_RANGE_KEY); 
	}
	
	public Future<Result<List<Result<byte[]>>>> getRangeKeyAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws TairRpcError, TairFlowLimit {
		return operateRangeKeyOrValueAsyncImpl(ns, pkey, begin, end, offset, maxCount, (reverse ? TairConstant.RANGE_KEY_ONLY_REVERSE : TairConstant.RANGE_KEY_ONLY), opt);
	}
	
	public Future<Result<List<Result<byte[]>>>> getRangeValueAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, TairOption opt) throws TairRpcError, TairFlowLimit {
		return operateRangeKeyOrValueAsyncImpl(ns, pkey, begin, end, offset, maxCount, (reverse ? TairConstant.RANGE_VALUE_ONLY_REVERSE : TairConstant.RANGE_VALUE_ONLY), opt);
	}
	
	public Future<Result<Map<String, String>>> getStatAsync(int qtype, String groupName, long serverId, TairOption opt) throws TairRpcError, TairFlowLimit {
		if (opt == null)
			opt = defaultOptions;
		
		if (!groupName.endsWith("\0"))
			groupName = groupName + "\0";
		//OK!!!
		QueryInfoRequest request = QueryInfoRequest.build(qtype, groupName, serverId);
		SocketAddress addr = TairUtil.cast2SocketAddress(master) ;
		return tairProcessor.callConfigServerAsync(addr, request, opt.getTimeout(), QueryInfoResponse.class, TairResultCastFactory.QUERY_INFO);
	}

	 public Future<ResultMap<byte[], Result<byte[]>>> simplePrefixGetMultiAsync(short ns, byte[] pkey, List<byte[]> skeys, TairOption opt) throws TairRpcError, TairFlowLimit {
		 if (opt == null)
			opt = defaultOptions;
		 SimplePrefixGetMultiRequest request = SimplePrefixGetMultiRequest.build(ns);
		 request.addKeys(pkey, skeys);
		 SocketAddress addr = tairProcessor.matchDataServer(TairConstant.PREFIX_KEY_TYPE, pkey);
		 Set<TairResultFutureImpl<SimplePrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>>> futureSet = new HashSet<TairResultFutureImpl<SimplePrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>>>();
			
		 TairResultFutureImpl<SimplePrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>> future =  tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), SimplePrefixGetMultiResponse.class, TairResultCastFactory.SIMPLE_PREFIX_GET_MULTI);
		 futureSet.add(future);
		 return new TairResultFutureSetImpl<SimplePrefixGetMultiResponse, byte[], ResultMap<byte[], Result<byte[]>>>(futureSet); 
	 }
			
		 
	public void notifyFuture(Future<?> future, Object ctx) throws TairQueueOverflow {
		TairResultFuture<?> rfuture = (TairResultFuture<?>)future;
		rfuture.setContext(ctx);
		if (notifyQueue.size() >= maxNotifyQueueSize) {
			throw new TairQueueOverflow("blocking queue is overflow.");
		}
		rfuture.futureNotify(notifyQueue);
	}
	
	public NotifyFuture poll(long timeout, TimeUnit unit) throws InterruptedException {
		TairResultFuture<?> future = notifyQueue.poll(timeout, unit);
		if (future == null)
			return null;
		return new NotifyFuture(future, future.getContext());
	}
	
	public NotifyFuture poll() throws InterruptedException {
		TairResultFuture<?> future = notifyQueue.poll();
		if (future == null)
			return null;
		return new NotifyFuture(future, future.getContext());
	}
	
	public Map<String, String> notifyStat() {
		Map<String, String> stat = new HashMap<String, String>();
    	stat.put("csversion", "" + this.tairProcessor.getServerManager().getConfigVersion());
    	stat.put("csgroup", getGroup());
    	stat.put("csaddress", "" + getMaster() + ", " + getSlave());
    	return stat;
	}
}
