package com.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyBizable extends VersionedProtocol {

	public static final long VERSION=24532451L;
	
	String hello(String name);
}
