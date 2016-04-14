package com.taobao.tair3.test.api;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.impl.DefaultTairClient;
public class TestBase {
	protected String master = ""; // tair master cs address, for example, 10.232.4.14:5008;
	protected String slave = null; // tair slave cs address
	protected String group = "group_1"; // tair group name
	protected DefaultTairClient tair = null;
	protected short ns = 120; //namespace
	protected TairOption opt = new TairOption(500/*timeout*/);
	@Before
	public void setUp() throws Exception {
		tair = new DefaultTairClient();
		tair.setMaster(master);
		tair.setSlave(slave);
		tair.setGroup(group);
		tair.init();
	}

	@After
	public void tearDown() throws Exception {
		 tair.close();
	}
	
	public List<byte[]> generateKeys (int count) {
		List<byte[]> r = new ArrayList<byte[]> ();
		for (int i = 0; i < count; ++i) {
			r.add(UUID.randomUUID().toString().getBytes());
		}
		return r;
	}
	public void removeKey(byte[] pkey, byte[] skey) {
		
	}
	public void removeKey(byte[] pkey, List<byte[]> skeys) {
		
	}
	public List<byte[]> generateOrderedKeys(byte[] key, int count) {
		return null;
	}
}
