package consistent;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

import consistent.ConsistentTimeClientWatcher;
import consistent.generated.ConsistentTimeService;

/**
 * consistentTimeClientWatcher listens to the change of master znode and always connect with the active
 * consistentTime server. 
 */
public class ConsistentTimeClientWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(ConsistentTimeClientWatcher.class);

  private final String zkQuorum;
  private final String baseZnode;
  private final String masterZnode;
  private final int sessionTimeout;
  private final int connectRetryTimes;
  
  private ZooKeeper zooKeeper;
  private TTransport transport;
  private TProtocol protocol;
  private ConsistentTimeService.Client client;
  protected  CountDownLatch waitInitLatch = new CountDownLatch(1);

  /**
   * Construct consistentTimeClientWatcher with properties.
   * 
   * @param properties the properties of consistentTimeClientWatcher
   * @throws IOException when error to connect ZooKeeper or consistentTimeServer
   */
  public ConsistentTimeClientWatcher(Properties properties) throws IOException {
    zkQuorum = properties.getProperty(ConsistentTimeClient.ZK_QUORUM, "127.0.0.1:2181");
    baseZnode = properties.getProperty(ConsistentTimeClient.CLUSTER_NAME, "default-cluster");
    masterZnode = baseZnode + "/master";
    sessionTimeout = Integer.parseInt(properties.getProperty(ConsistentTimeClient.SESSION_TIMEOUT, "5000"));
    connectRetryTimes = Integer.parseInt(properties.getProperty(ConsistentTimeClient.CONNECT_RETRY_TIMES, "10"));
    
    connectZooKeeper();
    connectConsistentTimeServer();
  }

  /**
   * Initialize ZooKeeper object and connect with ZooKeeper with retries.
   * 
   * @throws IOException when error to connect with ZooKeeper after retrying
   */
  private void connectZooKeeper() throws IOException {  
    for (int i = 0; i <= connectRetryTimes; i++) {
      try {
        zooKeeper = new ZooKeeper(zkQuorum, sessionTimeout, this);
        LOG.info("Connected ZooKeeper " + zkQuorum);
        break;
      } catch (IOException e) {
        if (i == connectRetryTimes) {
          throw new IOException("Can't connect ZooKeeper after retrying", e);
        }
        LOG.info("Exception to connect ZooKeeper, retry " + (i + 1) + " times");
      }
    }
  }

  /**
   * Reconnect with ZooKeeper.
   * 
   * @throws InterruptedException when interrupt close ZooKeeper object
   * @throws IOException when error to connect with ZooKeeper
   */
  private void reconnectZooKeeper() throws InterruptedException, IOException {
    LOG.info("Try to reconnect ZooKeeper " + zkQuorum);
    
    if (zooKeeper != null) {
      zooKeeper.close();
    }
    connectZooKeeper();
  }
  
  /**
   * Access ZooKeeper to get current master consistentTimeServer and connect with it.
   * 
   * @throws IOException when error to access ZooKeeper or connect with consistentTimeServer
   */
  private void connectConsistentTimeServer() throws IOException {
    LOG.info("Try to connect consistentTime server");
    
    byte[] hostPortBytes = getData(this, masterZnode);

    if (hostPortBytes != null) {
      String hostPort = new String(hostPortBytes); // e.g. 127.0.0.0_2181
      LOG.info("Find the active consistentTime server in " + hostPort);
      try {
        transport = new TSocket(hostPort.split("_")[0], Integer.parseInt(hostPort.split("_")[1]));
        transport.open();
        protocol = new TBinaryProtocol(transport);
        client = new ConsistentTimeService.Client(protocol);
      } catch (TException e) {
        new IOException("Exception to connect consistentTime server in " + hostPort);
      }
    } else {
      throw new IOException("The data of " + masterZnode + " is null");
    }
  }

  /**
   * Reconnect with consistentTimeServer.
   * 
   * @throws IOException when error to connect consistentTimeServer
   */
  private void reconnectconsistentTimeServer() throws IOException {
    LOG.info("Try to reconnect consistentTime server");
    
    if (transport != null) {
      transport.close();
    }
    connectConsistentTimeServer();
  }

  /**
   * Send RPC request to get timestamp from consistentTimeServer. Use lazy strategy to detect failure.
   * If request fails, reconnect consistentTimeServer. If request fails again, reconnect ZooKeeper.
   * 
   * @param range the number of timestamps
   * @return the first timestamp to use
   * @throws IOException when error to connect consistentTimeServer or ZooKeeper
   */
  public long getTimestamps(int range) throws IOException {
    long timestamp;
    try {
      timestamp = client.getTimestamps(range);
    } catch (TException e) {
      LOG.info("Can't get timestamp, try to connect the active consistentTime server");
      try {
        reconnectconsistentTimeServer();
        return client.getTimestamps(range);
      } catch (Exception e1) {
        LOG.info("Can't connect consistentTime server, try to connect ZooKeeper firstly");
        try {
          reconnectZooKeeper();
          reconnectconsistentTimeServer();
          return client.getTimestamps(range);
        } catch (Exception e2) {
          throw new IOException("Error to get timestamp after reconnecting ZooKeeper and consistentTime server", e2);
        }
      }
    }
    return timestamp;
  }
  
  /**
   * Provider the convenient method to get single timestamp.
   * 
   * @return the allocated timestamp
   * @throws IOException when error to get timestamp from consistentTimeServer
   */
  public long getTimestamp() throws IOException {
    return getTimestamps(1);
  }

  /**
   * Deal with connection event, just wait for a while when connected.
   * 
   * @param event ZooKeeper events
   */
  @Override
  public void process(WatchedEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.info("Received ZooKeeper Event, " + "type=" + event.getType() + ", " + "state="
          + event.getState() + ", " + "path=" + event.getPath());
    }

    switch (event.getType()) {
    case None: {
      switch (event.getState()) {
      case SyncConnected: {
        try {
          waitToInitZooKeeper(2000); // init zookeeper in another thread, wait for a while
          waitInitLatch.countDown();
        } catch (Exception e) {
          LOG.error("Error to init ZooKeeper object after sleeping 2000 ms, reconnect ZooKeeper");
          try {
            reconnectZooKeeper();
          } catch (Exception e1) {
            LOG.error("Error to reconnect with ZooKeeper", e1);
          }
        } 
        break;
      }
      default:
        break;
      }
      break;
    }
    default:
      break;
    }
  }
  
   /**
    * Wait to init ZooKeeper object, only sleep when it's null.
    *
    * @param maxWaitMillis the max sleep time
    * @throws Exception if ZooKeeper object is still null
    */
   public void waitToInitZooKeeper(long maxWaitMillis) throws Exception {
     long finished = System.currentTimeMillis() + maxWaitMillis;
     while (System.currentTimeMillis() < finished) {
       if (this.zooKeeper != null) {
         return;
       }
       
       try {
         Thread.sleep(1);
       } catch (InterruptedException e) {
         throw new Exception(e);
       }
     }
     throw new Exception();
   }


  /**
   * Get the data from znode.
   * 
   * @param consistentTimeClientWatcher the ZooKeeper watcher
   * @param znode the znode you want to access
   * @return the byte array of value in znode
   * @throws IOException when error to access ZooKeeper
   */
	public byte[] getData(ConsistentTimeClientWatcher consistentTimeClientWatcher,
			String znode) throws IOException {
		//当zookeeper连接初始化成功之后获取节点数据
		if (States.CONNECTING == zooKeeper.getState()) {
			LOG.info("zookeeper connecting...,pls wait!");
			try {
				waitInitLatch.await();
			} catch (InterruptedException e) {
				LOG.error(e);
			}
		}
		byte[] data = null;
		for (int i = 0; i <= connectRetryTimes; i++) {
			try {
				data = consistentTimeClientWatcher.getZooKeeper().getData(znode, null,
						null);
				break;
			} catch (Exception e) {
				LOG.info("Exceptioin to get data from ZooKeeper, retry " + i
						+ " times");
				if (i == connectRetryTimes) {
					throw new IOException("Error when getting data from "
							+ znode + " after retrying");
				}
			}
		}
		return data;
	}

  /**
   * Close the ZooKeeper object.
   */
  public void close() {
    if (zooKeeper != null) {
      try {
        zooKeeper.close();
      } catch (InterruptedException e) {
        LOG.error("Interrupt to close zookeeper connection", e);
      }
    }
  }
  
  public ZooKeeper getZooKeeper() {
    return zooKeeper;
  }

}
