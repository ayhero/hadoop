package com.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		final MyBizable proxy=(MyBizable) RPC.waitForProxy(MyBizable.class, MyBizable.VERSION, new InetSocketAddress(MyServer.SERVER_ADDRESS, MyServer.SERVER_PORT),new Configuration());
		
		final String result= proxy.hello("world");
		System.out.println("客户端调用结果:"+result);
		RPC.stopProxy(proxy);
	
	}

}
