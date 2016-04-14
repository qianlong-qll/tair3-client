/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair3.client.packets.dataserver;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.packets.AbstractResponsePacket;

public class GetResponse extends AbstractResponsePacket {
	
    protected List<Result<byte[]>> datas = null;
    protected List<Result<byte[]>> proxyDatas = null;
    public List<Result<byte[]>> getEntrires() {
    	return datas;
    }
    
    @Override
    public void decodeFrom(ChannelBuffer buff) {
        resultCode 		= buff.readInt();
        int count   = buff.readInt();
        int size = 0;

        //entries = new ArrayList<DataEntry>(count);
        datas = new ArrayList<Result<byte[]>>(count);
		for (int i = 0; i < count; i++) {
			Result<byte[]> r = new Result<byte[]>();
			
			decodeMeta(buff, r);
			int msize = buff.readInt();
			size = (msize & 0x3FFFFF);
			short prefixSize = (short)(msize >> 22);

			//with prefix key
			if (prefixSize != 0) {
				size -= PREFIX_KEY_TYPE.length;
				prefixSize -= PREFIX_KEY_TYPE.length;
				//two bytes flag
				//buff.readShort();
				buff.skipBytes(PREFIX_KEY_TYPE.length);
			}
			byte[] keyBytes = new byte[size];
			buff.readBytes(keyBytes);
			r.setKey(keyBytes, prefixSize);
		 
			decodeMeta(buff);
			int valLength = buff.readInt();
			byte[] valBytes = new byte[valLength];
			buff.readBytes(valBytes);
			if (r.isCounter()) {
				byte[] rowCount = new byte[4];
				System.arraycopy(valBytes, 2, rowCount, 0, 4);
				r.setResult(rowCount);
			}
			else {
				r.setResult(valBytes);
			}
			datas.add(r);
		}
        return ;
    }    
    
    
    public boolean hasConfigVersion() {
    	return true;
    }
}
