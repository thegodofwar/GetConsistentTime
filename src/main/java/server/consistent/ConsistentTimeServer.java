package consistent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import consistent.generated.ConsistentTimeService;
import consistent.zookeeper.FailoverServer;

/**
 * The thrift server for clients to get the precise auto-increasing timestamp. It relies on
 * ZooKeeper for active/backup servers switching and the max allocated timestamp is persistent in
 * ZooKeeper.
 * 
 * @see ConsistentTimeServerWatcher
 */
public class ConsistentTimeServer extends FailoverServer {
  private static final Log LOG = LogFactory.getLog(ConsistentTimeServer.class);

  public static final String CLUSTER_NAME = "clusterName";
  public static final String MAX_THREAD = "maxThread";
  public static final String ZK_ADVANCE_TIMESTAMP = "zkAdvanceTimestamp";

  private final Properties properties;
  private final ConsistentTimeServerWatcher consistentTimeServerWatcher;
  private ConsistentTimeImplement consistentTimeImplement;
  private TServer thriftServer;

  /**
   * Construct consistentTimeServer with consistentTimeServerWatcher and properties.
   *
   * @param consistentTimeServerWatcher the consistentTimeServerWatcher
   * @param properties the properties of consistentTimeServer
   * @throws TTransportException when error to init thrift server
   * @throws consistentTimeException when error to construct consistentTimeServerWatcher
   */
  public ConsistentTimeServer(final ConsistentTimeServerWatcher consistentTimeServerWatcher, Properties properties)
      throws Exception {
    super(consistentTimeServerWatcher);
    this.consistentTimeServerWatcher = consistentTimeServerWatcher;
    this.properties = properties;

    LOG.info("Init thrift server in " + consistentTimeServerWatcher.getHostPort().getHostPort());
    initThriftServer();
  }

  /**
   * Construct consistentTimeServer with consistentTimeServerWatcher.
   *
   * @throws TTransportException when error to init thrift server
   * @throws consistentTimeException when error to construct consistentTimeServerWatcher
   */
  public ConsistentTimeServer(final ConsistentTimeServerWatcher consistentTimeServerWatcher) throws TTransportException, Exception {
    this(consistentTimeServerWatcher, consistentTimeServerWatcher.getProperties());
  }

  /**
   * Construct consistentTimeServer with properties.
   *
   * @param properties the properties of consistentTimeServer
   * @throws TTransportException when error to init thrift server
   * @throws consistentTimeException when error to construct consistentTimeServerWatcher
   * @throws IOException when whe error to construct consistentTimeServerWatcher
   */
  public ConsistentTimeServer(Properties properties) throws TTransportException,Exception {
    this(new ConsistentTimeServerWatcher(properties), properties);
  }

  /**
   * Initialize Thrift server of consistentTimeServer.
   *
   * @throws TTransportException when error to initialize thrift server
   * @throws FatalconsistentTimeException when set a smaller timestamp in ZooKeeper
   * @throws consistentTimeException when error to set timestamp in ZooKeeper
   */
  private void initThriftServer() throws TTransportException, Exception {

    int maxThread = Integer.parseInt(properties.getProperty(MAX_THREAD,
      String.valueOf(Integer.MAX_VALUE)));
    String serverHost = properties.getProperty(FailoverServer.SERVER_HOST);
    int serverPort = Integer.parseInt(properties.getProperty(FailoverServer.SERVER_PORT));
    TServerSocket serverTransport = new TServerSocket(new InetSocketAddress(serverHost, serverPort));
    Factory proFactory = new TBinaryProtocol.Factory();

    consistentTimeImplement = new ConsistentTimeImplement(properties, consistentTimeServerWatcher);

    TProcessor processor = new ConsistentTimeService.Processor(consistentTimeImplement);
    Args rpcArgs = new Args(serverTransport);
    rpcArgs.processor(processor);
    rpcArgs.protocolFactory(proFactory);
    rpcArgs.maxWorkerThreads(maxThread);
    thriftServer = new TThreadPoolServer(rpcArgs);
  }

  /**
   * Initialize persistent timestamp and start to serve as active master.
   */
  @Override
  public void doAsActiveServer() {
    try {
      LOG.info("As active master, init timestamp from ZooKeeper");
      consistentTimeImplement.initTimestamp();
    } catch (Exception e) {
      LOG.fatal("Exception to init timestamp from ZooKeeper, exit immediately");
      System.exit(0);
    }
    
    consistentTimeServerWatcher.setBeenActiveMaster(true);

    LOG.info("Start to accept thrift request");
    startThriftServer();
  }

  /**
   * Stop thrift server of consistentTimeServer.
   */
  public void stopThriftServer() {
    if (thriftServer != null) {
      thriftServer.stop();
    }
  }

  /**
   * Start thrift server of consistentTimeServer.
   */
  public void startThriftServer() {
    if (thriftServer != null) {
      thriftServer.serve();
    }
  }

  /**
   * The main entrance of consistentTimeServer to start the service.
   *
   * @param args will ignore all the command-line arguments
   */
  public static void main(String[] args) {
    Properties properties = new Properties();
    LOG.info("Load consistentTime.cfg configuration from class path");
    try {
      properties.load(ConsistentTimeServer.class.getClassLoader().getResourceAsStream("consistentTime.cfg"));
      properties.setProperty(FailoverServer.BASE_ZNODE,
        "/consistentTime" + "/" + properties.getProperty(CLUSTER_NAME));
    } catch (IOException e) {
      LOG.fatal("Error to load consistentTime.cfg configuration, exit immediately", e);
      System.exit(0);
    }

    ConsistentTimeServer consistentTimeServer = null;
    LOG.info("Init consistentTime server and connect ZooKeeper");
    try {
      consistentTimeServer = new ConsistentTimeServer(properties);
    } catch (Exception e) {
      LOG.fatal("Exception to init consistentTime server, exit immediately", e);
      System.exit(0);
    }
    consistentTimeServer.run();
  }

}
