package org.wxstc.spark

import java.net.ServerSocket
import java.rmi.Naming
import java.rmi.registry.LocateRegistry

import org.apache.spark.sql.SparkSession
import org.wxstc.spark.network.server.TransportServer
import org.wxstc.spark.plugins.{DriverExecuteSparkImpl, ExecuteSparkJob, SparkJobRunnable}
import org.wxstc.spark.zookeeper.{DriverZkInfo, ZkClient}

object DriverElastic extends Serializable {
  def run(spark: SparkSession): Unit ={
    val conf = spark.conf

    val appName = conf.get("spark.app.name", "none")
    val driverHost = conf.get("spark.driver.host", "localhost")
//    val driverHost = conf.get("SPARK_LOCAL_IP", "localhost")
    val nettyPort = new ServerSocket(0).getLocalPort(); //读取空闲的可用端口

    val rmiPort = 1099//new ServerSocket(0).getLocalPort(); //读取空闲的可用端口

    val zkServer = conf.get("spark.driver.plugin.zkServer", "localhost:2181")
    val zkPath = conf.get("spark.driver.plugin.zkPath", "spark_elastic")
    val zkClient = new ZkClient(zkServer, zkPath)
    val info = DriverZkInfo.apply(appName, s"""$driverHost:$nettyPort""", s"""$driverHost:$rmiPort""")
    zkClient.registerServer(info)

    val job = new DriverExecuteSparkImpl(spark)
    LocateRegistry.createRegistry(rmiPort)
    Naming.bind("rmi://" + driverHost + ":" + rmiPort +"/executeSparkJob", job)
    new TransportServer(driverHost, nettyPort, spark)
  }

}
