title: 读取spark checkpoint的数据
date: 2014-09-01 15:18:58
categories: Spark
tags: [spark,checkpoint]
---
checkpoint会将结果写到hdfs上，当driver 关闭后数据不会被清除。所以可以在其他driver上重复利用该checkpoint的数据。

checkpoint write data:
```scala
    sc.setCheckpointDir("data/checkpoint")
    val rddt = sc.parallelize(Array((1,2),(3,4),(5,6)),2)
    rddt.checkpoint()
    rddt.count() //要action才能触发checkpoint
```

read from checkpoint data:
```scala
	package org.apache.spark

	import org.apache.spark.rdd.RDD

	object RDDUtilsInSpark {
	  def getCheckpointRDD[T](sc:SparkContext, path:String) = {
	  	//path要到part-000000的父目录
	    val result : RDD[Any] = sc.checkpointFile(path)
	    result.asInstanceOf[T]
	  }
	}
```
*note:因为sc.checkpointFile(path)是private[spark]的，所以该类要写在自己工程里新建的package org.apache.spark中*

example:
```scala
	val rdd : RDD[(Int, Int)]= RDDUtilsInSpark.getCheckpointRDD(sc, "data/checkpoint/963afe46-eb23-430f-8eae-8a6c5a1e41ba/rdd-0")
    println(rdd.count())
    rdd.collect().foreach(println)
```
这样就可以原样复原了。