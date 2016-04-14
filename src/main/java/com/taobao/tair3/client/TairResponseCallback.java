package com.taobao.tair3.client;

/**
 * Callback
 * @author tianmai.fh 
 * @date 2013-4-3
 */
public interface TairResponseCallback {
   
   public void callback(Result<?> resp) ;
   
   public void callback(Throwable e);
   
}
