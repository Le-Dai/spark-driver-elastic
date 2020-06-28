package org.wxstc.spark.jobImpl;

import org.wxstc.spark.plugins.SparkJob;

import java.rmi.RemoteException;
import java.util.ArrayList;

class SparkRunImpl extends SparkJob {
    override def run(): Unit = {
        val spark1 = this.spark
        import spark1.implicits._
        val frame = spark1.sparkContext.parallelize(1 to 10).toDF("id")
        frame.printSchema()
        frame.show()
    }
}
