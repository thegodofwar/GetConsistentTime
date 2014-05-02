package main;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import consistent.ConsistentTimeClient;

public class StartClient {
    
	/**
	 * 客户端首先连接上zookeeper,从/GetConsistentTime/master这个临时节点获得thrift server的ip_port信息,<br/>
	 * thrift client根据ip_port连接到当前的server,最后获取时间服务
	 * 
	 * @author thegodofwar
	 */
	public static void main(String[] argv) {
		try {
			ConsistentTimeClient consistentTimeClient = new ConsistentTimeClient(
					"127.0.0.1:2181", "/GetConsistentTime");
			long ct = consistentTimeClient.getTimestamp();
			System.out.println("Get timestamp " + ct);
			DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Timestamp t = new Timestamp(ct);
			System.out.println(sdf.format(t));
		} catch (IOException e) {
			System.err
					.println("Error to connect with ZooKeeper or ConsistentTimeServer, check the configuration");
		}
	}

}
