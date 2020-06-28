package org.wxstc.spark.client

import java.io.{BufferedInputStream, InputStream}
import java.rmi.Naming
import java.util

import org.apache.hadoop.util.hash.Hash
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.wxstc.spark.jobImpl.SparkRunImpl
import org.wxstc.spark.network.protocol.LoadClassRequest
import org.wxstc.spark.plugins.{ExecuteSparkJob, JobImpl, SparkJob, SparkJobRunnable, SparkJobRunnableSuite}
import org.wxstc.spark.utils.{ByteArrayUtils, ClassDependenciesUtils, MemoryClassLoader}
import scala.collection.JavaConverters._

class RemoteSparkSessionTest extends FunSuite with Serializable {
  Thread.currentThread().setContextClassLoader(new MemoryClassLoader(new util.HashMap[String, Array[Byte]]()))

  test("spark remote test"){
    val session = new RemoteSparkSession("test5562", "sinan01:2181")
    val trun = new SparkJob {
      override def run(): Unit = {
        val session = spark
        import session.implicits._
        val frame = session.sparkContext.parallelize(1 to 10).toDF("id")
        frame.printSchema()
        frame.show()
      }
    }
    session.sendRpcDriverAction(trun)

//    val job = new SparkRunImpl()
//    session.executeSparkJob(job)
//    session.transportClient.getChannelRef.get().writeAndFlush(run.getClass)
//    session.sendRpcDriverAction(j2)
  }

  test("classloader"){

  }

  test("client rmi"){
    val job = Naming.lookup("rmi://localhost:1099/executeSparkJob").asInstanceOf[ExecuteSparkJob]

    val run = new JobImpl()
    System.out.println(job.run(run))
  }

  test("loadlclass"){

    val trun = new SparkJob {
      override def run(): Unit = {
        val session = spark
        import session.implicits._
        val frame = session.sparkContext.parallelize(1 to 10).toDF("id")
        frame.printSchema()
        frame.show()
      }
    }

    val clazz = trun.getClass

    println(1)

  }


  test("test gaga"){
    val trun = new SparkJob {
      override def run(): Unit = {
        val session = spark
        import session.implicits._
        val frame = session.sparkContext.parallelize(1 to 10).toDF("id")
        frame.printSchema()
        frame.show()
      }
    }


    val value = ClassDependenciesUtils.getDependencies(trun.getClass)

    val stringToBytes1 = ClassDependenciesUtils.getDependenciesBinary(trun.getClass)
    val stringToBytes = new util.HashMap[String, Array[Byte]]()
    value.asScala.foreach(c => {
      if(!c.getName.startsWith("java") && !c.getName.startsWith("scala")) stringToBytes.put(c.getName, ByteArrayUtils.getClassBytes(c))
    })

    println(stringToBytes)
  }
}
