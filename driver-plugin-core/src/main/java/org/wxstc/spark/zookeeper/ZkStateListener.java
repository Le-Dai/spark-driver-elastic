package org.wxstc.spark.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

public class ZkStateListener implements ConnectionStateListener {
    public ZkClient zkClient;
    public ZkStateListener(ZkClient zkClient){
        this.zkClient = zkClient;
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        if (connectionState == ConnectionState.LOST) {
            while (true) {
                try {
                    if (curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
                        zkClient.registerServer(zkClient.driverZkInfo);
                        break;
                    }
                } catch (InterruptedException e) {
                    //TODO: log something
                    break;
                } catch (Exception e) {
                    //TODO: log something
                }
            }
        }
    }
}
