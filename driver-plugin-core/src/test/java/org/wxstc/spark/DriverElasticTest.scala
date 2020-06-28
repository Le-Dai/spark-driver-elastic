package org.wxstc.spark

import java.rmi.Naming
import java.rmi.registry.LocateRegistry

import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.wxstc.spark.plugins.{ExecuteSparkJob, SparkJobRunnable}
import org.wxstc.spark.utils.ByteArrayUtils

class DriverElasticTest extends org.scalatest.FunSuite with Serializable {

  test("driver elastic run main"){
    val sparkConf = new SparkConf()
      .set(CATALOG_IMPLEMENTATION.key, "in-memory")
      .set("dfs.client.block.write.replace-datanode-on-7failure.policy", "ALWAYS")
      .set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    //this setting will set driver netty register
      .set("spark.driver.plugin.zkServer", "sinan01:2181")
      .set("spark.driver.plugin.zkPath", "spark_elastic")
      .set("spark.app.name", "dltest1")

    sparkConf.set("spark.master", sparkConf.get("spark.master","local[*]"))

    val builder = SparkSession
      .builder()
      .config(sparkConf)

    val spark = builder.getOrCreate()
    DriverElastic.run(spark)
  }

  test("rmi server"){
    val bytes = ByteArrayUtils.getClassBytes(classOf[JobImpl])
//    val job = new JobImpl();
//    LocateRegistry.createRegistry(1099)
//    Naming.bind("rmi://" + "localhost" + ":" + 1099 +"/executeSparkJob", job)
  }
}
