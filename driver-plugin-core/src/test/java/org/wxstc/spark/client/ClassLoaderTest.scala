package org.wxstc.spark.client

import java.util
import java.util.HashMap

import org.scalatest.FunSuite
import org.wxstc.spark.plugins.SparkJob
import org.wxstc.spark.utils.MemoryClassLoader

class ClassLoaderTest extends FunSuite{
  private val loader = new MemoryClassLoader(new util.HashMap[String, Array[Byte]])
  Thread.currentThread().setContextClassLoader(loader)

  def mm1(): Unit ={
    val run = new SparkJob {
      override def run(): Unit = {
        val session = spark
        import session.implicits._
        val frame = session.sparkContext.parallelize(1 to 10).toDF("id")
        frame.printSchema()
        frame.show()
      }
    }

    new Thread(new Runnable {
      Thread.currentThread().setContextClassLoader(loader)
      override def run(): Unit = {
//        val value1 = loader.loadClass("org.wxstc.spark.client.SparkRunjobImpl2")
        val value = Class.forName("org.wxstc.spark.plugins.SparkJob")
        val value2 = Class.forName("org.wxstc.spark.client.SparkRunjobImpl2")

        println(1)
      }
    }).start()

  }
  test("memory loader"){
    val unit = new ClassLoaderTest().mm1()
    unit

  }
}
