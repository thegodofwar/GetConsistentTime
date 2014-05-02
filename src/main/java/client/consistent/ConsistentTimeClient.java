package consistent;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;

import consistent.ConsistentTimeClient;
import consistent.ConsistentTimeClientWatcher;

/**
 * The client of ConsistentTimeServer provides an interface to get precise auto-increasing timestamp. It
 * will throw IOException for any error during connecting with server or getting timestamp.
 * 
 * @see ConsistentTimeClientWatcher
 */
public class ConsistentTimeClient {
  
  public static final String ZK_QUORUM = "zkQuorum";
  public static final String CLUSTER_NAME = "clusterName";
  public static final String SESSION_TIMEOUT = "sessionTimeout";
  public static final String CONNECT_RETRY_TIMES = "connectRetryTimes";
  
  private ConsistentTimeClientWatcher consistentTimeClientWatcher;

  /**
   * Construct consistentTimeClient with consistentTimeClientWatcher.
   * 
   * @param consistentTimeClientWatcher the consistentTimeClientWatcher object
   */
  public ConsistentTimeClient(ConsistentTimeClientWatcher consistentTimeClientWatcher) {
    this.consistentTimeClientWatcher = consistentTimeClientWatcher;
  }
  
  /**
   * Construct consistentTimeClient with properties.
   * 
   * @param properties the properties of consistentTimeClient
   * @throws IOException when error to construct consistentTimeClientWatcher
   */
  public ConsistentTimeClient(Properties properties) throws IOException {
    this.consistentTimeClientWatcher = new ConsistentTimeClientWatcher(properties);
  }
  
  /**
   * Construct consistentTimeClient just with ZkQuorum and clusterName, use default properties.
   * 
   * @param zkQuorum the ZooKeeper quorum string
   * @throws IOException when error to construct consistentTimeClientWatcher
   */
  public ConsistentTimeClient(String zkQuorum, String clusterName) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(ZK_QUORUM, zkQuorum);
    properties.setProperty(CLUSTER_NAME, clusterName);
    properties.setProperty(SESSION_TIMEOUT, String.valueOf(30000));
    properties.setProperty(CONNECT_RETRY_TIMES, String.valueOf(10));
    this.consistentTimeClientWatcher = new ConsistentTimeClientWatcher(properties);
  }

  /**
   * Get timestamps from consistentTimeClientWatcher.
   * 
   * @param range the number of timestamps
   * @return the first timestamp to use
   * @throws IOException when error to connect consistentTimeServer or ZooKeeper
   */
  public long getTimestamps(int range) throws IOException {
    return consistentTimeClientWatcher.getTimestamps(range);
  }
  
  /**
   * Get timestamp from consistentTimeClientWatcher.
   * 
   * @return the timestamp to use
   * @throws IOException  when error to connect consistentTimeServer or ZooKeeper
   */
  public long getTimestamp() throws IOException {
    return consistentTimeClientWatcher.getTimestamp();
  }

  /**
   * The command-line tool to use consistentTimeClient to get a timestamp.
   * Usage: mvn exec:java -Dexec.mainClass="com.xiaomi.infra.consistentTime.client.consistentTimeClient" -Dexec.args="$zkQuorum $clusterName"
   * 
   * @param argv first argument is ZooKeeper quorum string
   */
  public static void main(String[] argv) {
    try {
      ConsistentTimeClient consistentTimeClient = new ConsistentTimeClient("127.0.0.1:2181", "/failover-server");
      long ct = consistentTimeClient.getTimestamp();
      System.out.println("Get timestamp " + ct);
      DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"); 
      Timestamp t = new Timestamp(ct);
      System.out.println(sdf.format(t));
    } catch (IOException e) {
      System.err.println("Error to connect with ZooKeeper or consistentTimeServer, check the configuration");
    }
  }

  public ConsistentTimeClientWatcher getconsistentTimeClientWatcher(){
    return consistentTimeClientWatcher;
  }
  
}
