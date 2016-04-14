package com.taobao.tair3.client.impl.cast;

public class TairResultCastFactory {
	public static AddCountCast ADD_COUNT = new  AddCountCast();
	public static AddCountBoundedCast ADD_COUNT_BOUNDED = new  AddCountBoundedCast();
	public static BatchDeleteCast BATCH_DELETE = new BatchDeleteCast(); 
	public static BatchGetCast BATCH_GET = new BatchGetCast();
	public static BatchInvalidCast BATCH_INVALID = new BatchInvalidCast();
	public static BatchLockKeyCast BATCH_LOCK_KEY = new BatchLockKeyCast();
	public static BatchPrefixGetMultiCast  BATCH_PREFIX_GET_MULTI = new BatchPrefixGetMultiCast();
	public static BatchPrefixGetHiddenMultiCast BATCH_PREFIX_GET_HIDDEN_MULTI = new BatchPrefixGetHiddenMultiCast();
	public static BatchPutCast BATCH_PUT =  new BatchPutCast();
	public static BatchPutOldCast BATCH_PUT_OLD = new BatchPutOldCast();
	public static DeleteCast DELETE =  new DeleteCast();
	public static ExpireCast EXPIRE =  new ExpireCast();
	public static GetCast GET = new GetCast();
	public static GetHiddenCast GET_HIDDEN = new GetHiddenCast();
	public static GetRangeCast GET_RANGE = new GetRangeCast();
	public static HideByProxyCast HIDE_BY_PROXY = new HideByProxyCast();
	public static HideCast HIDE = new HideCast();
	public static InvalidCast INVALID = new InvalidCast();
	public static LockKeyCast LOCK_KEY = new LockKeyCast();
	public static PrefixAddCountMultiCast PREFIX_ADD_COUNT_MULTI =  new PrefixAddCountMultiCast();
	public static PrefixAddCountBoundedMultiCast PREFIX_ADD_COUNT_BOUNDED_MULTI =  new PrefixAddCountBoundedMultiCast();
	public static PrefixDeleteMultiCast PREFIX_DELETE_MULTI =  new PrefixDeleteMultiCast();
	public static PrefixGetHiddenMultiCast PREFIX_GET_HIDDEN_MULTI =  new PrefixGetHiddenMultiCast();
	public static PrefixGetMultiCast PREFIX_GET_MULTI = new PrefixGetMultiCast();
	public static PrefixHideMultiByProxyCast PREFIX_HIDE_MULTI_BY_PROXY = new PrefixHideMultiByProxyCast();
	public static PrefixHideMultiCast PREFIX_HIDE_MULTI = new PrefixHideMultiCast();
	public static PrefixInvalidMultiCast PREFIX_INVALID_MULTI = new PrefixInvalidMultiCast();
	public static PrefixPutMultiCast PREFIX_PUT_MULTI = new PrefixPutMultiCast();
	public static GetRangeKeyCast GET_RANGE_KEY = new GetRangeKeyCast();
	public static GetRangeValueCast GET_RANGE_VALUE = new GetRangeValueCast();
	public static PutCast PUT = new PutCast();
	public static QueryInfoCast QUERY_INFO = new QueryInfoCast();
	public static SimplePrefixGetMultiCast SIMPLE_PREFIX_GET_MULTI = new SimplePrefixGetMultiCast();
}
