package org.wxstc.spark.client

import java.net.InetSocketAddress
import java.rmi.Naming

import com.google.gson.Gson
import org.wxstc.spark.network.client.TransportClient
import org.wxstc.spark.network.protocol.DriverRpcRequest
import org.wxstc.spark.plugins.{ExecuteSparkJob, SparkJob, SparkJobRunnable}
import org.wxstc.spark.zookeeper.{DriverZkInfo, ZkClient}

class RemoteSparkSession(appName: String, zkServer: String, zkPath: String = "spark_elastic"){
  val driverInfo = new ZkClient(zkServer, zkPath).getDriverAddress(appName)
  val info = new Gson().fromJson(driverInfo, classOf[DriverZkInfo])

  val job = Naming.lookup(s"rmi://${info.rmi_end_point}/executeSparkJob").asInstanceOf[ExecuteSparkJob]

  val transportClient: TransportClient = {
    val nettyAddress = info.end_point
    if(nettyAddress == null || "".equals(nettyAddress)){
      throw new Exception("no live driver found")
    }else {
      val host = nettyAddress.split(":")(0)
      val port = nettyAddress.split(":")(1).toInt
      new TransportClient(new InetSocketAddress(host, port))
    }
  }

  def sendRpcDriverAction(runnable: SparkJobRunnable): Unit = {
    transportClient.sendAction(new DriverRpcRequest(runnable))
  }

  def executeSparkJob(run: SparkJob): Unit = {
    job.run(run)
  }
}