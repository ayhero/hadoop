package com.rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

public class MyBiz implements MyBizable{
 
	public String hello(String name){
		return "hello "+name;
	}

	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return this.VERSION;
	}

}
