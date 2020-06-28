# Spark Driver Plugin (Make your driver more flexible)

## Driver config

```scala
scalaval sparkConf = new SparkConf()
      .set("spark.driver.plugin.zkServer", "sinan01:2181")
  		// You can not configure it and use the default configuration zk path /spark_elastic
      .set("spark.driver.plugin.zkPath", "spark_elastic")
  		// This is the only representation of the task to ensure that the client connects to the correct server
      .set("spark.app.name", "dltest1")
  
//This will keep your driver resident to receive client tasks
DriverElastic.run(spark)
```

## Client submit job

```scala
//This will open a spark driver connection for dynamically submitting tasks
//test5562 app.name

//You need to extends SparkJob
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
```

