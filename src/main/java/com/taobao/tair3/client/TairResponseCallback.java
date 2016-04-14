package com.taobao.tair3.client;

/**
 * Callback
 * @author tianmai.fh 
 */
public interface TairResponseCallback {
   
   public void callback(Result<?> resp) ;
   
   public void callback(Throwable e);
   
}
