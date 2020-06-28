package org.wxstc.spark.zookeeper;

import com.google.gson.Gson;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

public class ZkClient {
    private CuratorFramework client = null;
    private final String zkServerPath;
    private final String nodePath;
    public DriverZkInfo driverZkInfo;

    public ZkClient(String zkServerPath, String nodePath) {
        this.zkServerPath = zkServerPath;
        this.nodePath = nodePath;
        RetryPolicy retryPolicy = new RetryNTimes(5, 5000);

        client = CuratorFrameworkFactory.builder()
                .connectString(zkServerPath)
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy)
                .namespace(nodePath).build();  //指定命名空间后，client 的所有路径操作都会以/workspace 开头
        client.getConnectionStateListenable().addListener(new ZkStateListener(this));
        client.start();
    }

    public void registerServer(DriverZkInfo info){
        this.driverZkInfo = info;
        String data = new Gson().toJson(info);
        try {
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath("/"+info.appName(), data.getBytes("utf-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getDriverAddress(String appcationName){
        String dataPath = "/"+appcationName;
        Stat stat = new Stat();
        /**
         * czxid	数据节点创建时的事务 ID
         * ctime	数据节点创建时的时间
         * mzxid	数据节点最后一次更新时的事务 ID
         * mtime	数据节点最后一次更新时的时间
         * pzxid	数据节点的子节点最后一次被修改时的事务 ID
         * cversion	子节点的更改次数
         * version	节点数据的更改次数
         * aversion	节点的 ACL 的更改次数
         * ephemeralOwner	如果节点是临时节点，则表示创建该节点的会话的 SessionID；如果节点是持久节点，则该属性值为 0
         * dataLength	数据内容的长度
         * numChildren	数据节点当前的子节点个数
         */
        try {
            byte[] data = client.getData().storingStatIn(stat).forPath(dataPath);
            return new String(data, "utf-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
