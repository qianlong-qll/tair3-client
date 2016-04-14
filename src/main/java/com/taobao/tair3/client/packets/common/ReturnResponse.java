/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair3.client.packets.common;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractResponsePacket;


public class ReturnResponse extends AbstractResponsePacket {
	
    //private int    code = 0;
    private String msg  = null;


    public boolean hasConfigVersion() {
    	return true;
    }
    @Override
    public void decodeFrom(ChannelBuffer buffer) {
        this.resultCode          = buffer.readInt();
        
        int len = buffer.readInt();

        if (len <= 1) {
            msg = "";
        } else {
            byte[] b = new byte[len];
            buffer.readBytes(b);
            msg = new String(b, 0, len - 1);
        }
    }
  
    /**
     * 
     * @return the msg
     */
    public String getMsg() {
        return msg;
    }
    
	public int decodeConfigVersionFrom(ChannelBuffer bb) {
		return bb.readInt();
	}
}
