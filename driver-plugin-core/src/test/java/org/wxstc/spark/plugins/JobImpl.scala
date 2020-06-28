package org.wxstc.spark.plugins;

import java.rmi.RemoteException;

class JobImpl extends SparkJob {
    override def run(): Unit = {
      val l = this.spark.sparkContext.parallelize(1 to 10).count()
      println(l)
    }
}
