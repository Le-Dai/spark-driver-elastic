package org.wxstc.spark.client

import org.wxstc.spark.plugins.SparkJob

class SparkRunjobImpl2 extends SparkJob {
  override def run(): Unit = {
    val spark1 = this.spark
    import spark1.implicits._
    val frame = spark1.sparkContext.parallelize(1 to 200).toDF("id")
    frame.printSchema()
    frame.show()
  }
}
