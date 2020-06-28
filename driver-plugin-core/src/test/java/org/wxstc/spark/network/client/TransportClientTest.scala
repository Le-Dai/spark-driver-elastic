package org.wxstc.spark.network.client

import java.net.{InetSocketAddress, SocketAddress}

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.wxstc.spark.network.protocol.DriverRpcRequest
import org.wxstc.spark.plugins.{SparkJobRunnable, SparkJobRunnableSuite}

class TransportClientTest extends FunSuite{
  test("driver elastic run main"){
    val client = new TransportClient(new InetSocketAddress("localhost", 9090))
//    val request = new DriverRpcRequest(new SparkJobRunnableSuite())

//    client.sendAction(request)
  }

}
