package com.taobao.tair3.client.util;

public class TairConstant {
	
	public static int  NAMESPACE_MAX = Short.MAX_VALUE;
	 
	public static int TAIR_ITEM_FLAG_ADDCOUNT = 1;
	public static int TAIR_ITEM_FLAG_DELETED = 2;
	public static int TAIR_ITEM_FLAG_ITEM = 4;
	public static int TAIR_ITEM_FLAG_LOCKED = 8;
	public static int TAIR_ITEM_FLAG_HAS_NEXT = 8;

	public static short TAIR_STYPE_MIXEDKEY = 12;
	public static short TAIR_STYPE_BYTEARRAY = 9;
	public static final int TAIR_STYPE_INCDATA = 11;
	public static final short PREFIX_KEY_OFFSET = 22;
	//get range
	public static final short RANGE_ALL  = 1;
	public static final short RANGE_ALL_REVERSE  = 4;
    public static final short RANGE_VALUE_ONLY  = 2;
    public static final short RANGE_VALUE_ONLY_REVERSE  = 5;
    public static final short RANGE_KEY_ONLY  = 3;
    public static final short RANGE_KEY_ONLY_REVERSE = 6;
    public static final short RANGE_DEL = 7;
    public static final short RANGE_DEL_REVERSE = 8;
    
    
    public static final int MAX_KEY_SIZE = 1023; //1k
    public static final int MAX_VALUE_SIZE = 1000000;
    
    public static final String NS_NOT_AVAILABLE = "ns is not available";
    public static final String KEY_NOT_AVAILABLE = "key is not available";
    public static final String KEY_SIZE_NOT_AVAILABLE = "key size is not available";
    public static final String VALUE_NOT_AVAILABLE = "value is not available";
    public static final String VALUE_SIZE_NOT_AVAILABLE = "value size is not available";
    public static final String OPTION_NOT_AVAILABLE = "option is not available";
    public static final String PREFIX_SIZE_NOT_AVAILABLE = "prefixSize is not available";
    public static final String MAP_NOT_AVAILABLE = "map is not available";
    public static final String LIST_NOT_AVAILABLE = "list is not available";
    public static final String ITEM_VALUE_NOT_AVAILABLE = "item value not available";
    public static final String EXPIRE_TIME_NOT_AVAILABLE = "expire time not available";
    public static final byte[] PREFIX_KEY_TYPE = new byte[2];
    static {
    	PREFIX_KEY_TYPE[1] = (byte) ((12 << 1) & 0xFF);
    	PREFIX_KEY_TYPE[0] = (byte) (((12 << 1) >> 8) & 0xFF);
    }
}
