package main;

import java.io.IOException;
import java.util.Properties;

import org.apache.thrift.transport.TTransportException;

import consistent.ConsistentTimeServer;
import consistent.ConsistentTimeServerWatcher;
import consistent.zookeeper.FailoverServer;
import consistent.zookeeper.HostPort;

public class StartServer {
    /**
     * 修改hostPort启动多个StartServer,第一个启动的会作为master给thrift client提供<br/>
     * 获取单调递增、一致的时间服务,后续启动的StartServer会wait住作为backup-servers,<br/>
     * 一旦master宕机/退出或者异常停止服务,backup-servers会通过Zookeeper的Watcher<br/>
     * 通过回调函数(/GetConsistentTime/master临时节点被删除触发NodeDataChanged事件)<br/>
     * notifyAll唤醒wait住的StartServer主线程重新选主(创建新的/GetConsistentTime/master<br/>
     * 临时节点,保存最新的thrift server的ip_port).这样即实现master挂了的情况，backup-servers会<br/>
     * 自动选择其中一个再次作为master,继续提供服务的容灾效果.
     * 
     * @author thegodofwar
     * 
     */
	public static void main(String args[]) {
		//这里可以修改端口号启动多个backup-servers
	    HostPort hostPort = new HostPort("127.0.0.1", 10086);//10086、10087、10088...
		Properties properties = new Properties();
		//FailoverServer参数
		properties.setProperty(FailoverServer.BASE_ZNODE, "/GetConsistentTime");
		//Zookeeper服务ip和端口号
		properties.setProperty(FailoverServer.ZK_QUORUM, "127.0.0.1:2181");
		properties.setProperty(FailoverServer.SERVER_HOST, hostPort.getHost());
		properties.setProperty(FailoverServer.SERVER_PORT, String.valueOf(hostPort.getPort()));
		properties.setProperty(FailoverServer.SESSION_TIMEOUT, "5000");
		properties.setProperty(FailoverServer.CONNECT_RETRY_TIMES, String.valueOf(10));
		//ChronosServer参数
		properties.setProperty(ConsistentTimeServer.MAX_THREAD, "1000");
		properties.setProperty(ConsistentTimeServer.ZK_ADVANCE_TIMESTAMP, "10000");
		ConsistentTimeServerWatcher consistentTimeServerWatcher = null;
		try {
			consistentTimeServerWatcher = new ConsistentTimeServerWatcher(properties, true);
		} catch (IOException e) {
		   e.printStackTrace();
		}
		ConsistentTimeServer consistentTimeServer = null;
		try {
			consistentTimeServer = new ConsistentTimeServer(consistentTimeServerWatcher);
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		consistentTimeServer.run();
//		
		
  }

}
