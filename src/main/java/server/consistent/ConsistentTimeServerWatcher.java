package consistent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import consistent.zookeeper.FailoverWatcher;
import consistent.zookeeper.ZooKeeperUtil;

/**
 * The ZooKeeper watcher for consistentTimeServer provides the interface to get the persistent timestamp
 * which is the max value allocated currently.
 */
public class ConsistentTimeServerWatcher extends FailoverWatcher {
  private static final Log LOG = LogFactory.getLog(ConsistentTimeServerWatcher.class);

  private final String persistentTimestampZnode;
  private boolean beenActiveMaster = false;
  private long persistentTimestamp;

  /**
   * Construct consistentTimeServerWatcher with properties.
   *
   * @param properties the properties of consistentTimeServerWatcher
   * @param canInitZnode whether it can create znode or not
   * @throws IOException when it's error to construct ZooKeeper watcher
   */
  public ConsistentTimeServerWatcher(Properties properties, boolean canInitZnode) throws IOException {
    super(properties, canInitZnode);
    
    persistentTimestampZnode = baseZnode + "/persistent-timestamp";
  }

  /**
   * Construct consistentTimeServerWatcher with properties.
   *
   * @param properties the properties of consistentTimeServerWatcher
   * @throws IOException when it's error to construct ZooKeeper watcher
   */
  public ConsistentTimeServerWatcher(Properties properties) throws IOException {
    this(properties, true);
  }

  /**
   * Create the base znode of consistentTime.
   */
  @Override
  protected void initZnode() { 
    try {
      super.initZnode();
      ZooKeeperUtil.createAndFailSilent(this, baseZnode + "/persistent-timestamp");
    } catch (Exception e) {
      e.printStackTrace();
      LOG.fatal("Error to create znode of consistentTime, exit immediately");
      System.exit(0);
    }
  }

  /**
   * Get persistent timestamp in ZooKeeper with retries.
   *
   * @return the persistent timestamp in ZooKeeper
   * @throws consistentTimeException error to get data from ZooKeeper after retrying
   */
  public long getPersistentTimestamp() throws Exception {
    byte[] persistentTimesampBytes = null;
    for (int i = 0; i <= connectRetryTimes; ++i) {
      try {
        persistentTimesampBytes = ZooKeeperUtil.getDataAndWatch(this, persistentTimestampZnode);
        break;
      } catch (KeeperException e) {
        if (i == connectRetryTimes) {
          throw new Exception(
              "Can't get persistent timestamp from ZooKeeper after retrying", e);
        }
        LOG.info("Exception to get persistent timestamp from ZooKeeper, retry " + (i + 1) + " times");
      }
    }

    if (Arrays.equals(persistentTimesampBytes, new byte[0])) {
      persistentTimestamp = 0; // for the very first time
    } else {
      persistentTimestamp = ZooKeeperUtil.bytesToLong(persistentTimesampBytes);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Return persistent timestamp " + persistentTimestamp + " to timestamp server");
    }
    return persistentTimestamp;
  }

  /**
   * Get the cached timestamp in consistentTimeServerWatcher.
   *
   * @return the local persistentTimestamp
   */
  public long getCachedPersistentTimestamp() {
    return persistentTimestamp;
  }

  /**
   * Set persistent timestamp in ZooKeeper.
   *
   * @param newTimestamp the new value to set
   * @throws FatalconsistentTimeException if try to set a small value
   * @throws consistentTimeException if error to set value in ZooKeeper
   */
  public void setPersistentTimestamp(long newTimestamp) throws Exception {
    if (newTimestamp <= persistentTimestamp) {
      throw new Exception("Fatal error to set a smaller timestamp");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting persistent timestamp " + newTimestamp + " in ZooKeeper");
    }

    for (int i = 0; i <= connectRetryTimes; ++i) {
      try {
        ZooKeeperUtil.setData(this, persistentTimestampZnode, ZooKeeperUtil.longToBytes(newTimestamp));
        persistentTimestamp = newTimestamp;
        break;
      } catch (KeeperException e) {
        if (i == connectRetryTimes) {
          throw new Exception(
              "Error to set persistent timestamp in ZooKeeper after retrying", e);
        }
        LOG.info("Exception to set persistent timestamp in ZooKeeper, retry " + (i + 1) + " times");
      }
    }
  }

  /**
   * Deal with the connection event.
   *
   * @param event the ZooKeeper event.
   */
  @Override
  protected void processConnection(WatchedEvent event) {
    super.processConnection(event);
    switch (event.getState()) {
    case Disconnected:
      if (beenActiveMaster) {
        LOG.fatal(hostPort.getHostPort()
            + " disconnected from ZooKeeper, stop serving and exit immediately");
        System.exit(0);
      } else {
        LOG.warn(hostPort.getHostPort()
            + " disconnected from ZooKeeper, wait to sync and try to become active master");
      }
      break;
    default:
      break;
    }
  }

  public String getPersistentTimestampZnode(){
    return persistentTimestampZnode;
  }
  
  public void setBeenActiveMaster(boolean beenActiveMaster) {
    this.beenActiveMaster = beenActiveMaster;
  }
  
}