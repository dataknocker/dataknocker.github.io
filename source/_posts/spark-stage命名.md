title: spark stage命名原理
date: 2014-09-11 19:45:16
categories: spark
tags: [spark,stage]
---

sparkUI中的stage名称可以方便进行程序逻辑分析，但如果不了解stage的命名原理，看sparkUI时有时就会一头雾水。

stage的名称来自两个地方：callSite (callSite = getCallSite = Utils.getCallSiteInfo) 与 rdd的creationSiteInfo (creationSiteInfo= Utils.getCallSiteInfo, 创建rdd时都会初始化该值)。
finalStage会有callSite(在runJob是会传递该参数)，其他stage则使用其rdd的creationSiteInfo，即
```java
	val name = callSite.getOrElse(rdd.getCreationSite)
```
不管是callSite还是rdd的creationSiteInfo，都是调用Utils.getCallSiteInfo方法。
其主要原理是获得调用栈，找到栈中是 spark core包(更具体的是符合正则SPARK_CLASS_REGEX的package)的最外层调用作为分界，该调用的方法为lastSparkMethod，然后找到下一个调用，即调用了lastSparkMethod的记录，从而得到firstUserFile、firstUserLine。
stage命名方式为：
```java
	"%s at %s:%s".format(lastSparkMethod, firstUserFile, firstUserLine)
```
SPARK_CLASS_REGEX代码：
```java
	SPARK_CLASS_REGEX = """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?\.[A-Z]""".r
```
<font color="red">总的说来就是找到调用 满足SPARK_CLASS_REGEX正则的package 中的方法的入口，该方法即为lastSparkMethod， 而该入口所在的类以及行就是firstUserFile, firstUserLine。</font>

## 例子
### 简单例子
以一个例子来说明上面的原理
```scala
	object StageNameTest {
	  def main(args:Array[String]){
	    val sc = new SparkContext("local","stageNameTest")
	    val rdd = sc.parallelize(1 to 100, 5)
	    println(rdd.count())
	  }
	}
```
#### rdd的creationSiteInfo
在sc.parallelize(1 to 100, 5)时会生成ParallelCollectionRDD，其creationSiteInfo的调用栈为：
```java
	org.apache.spark.util.Utils$.getCallSiteInfo(Utils.scala:817)
	org.apache.spark.rdd.RDD.<init>(RDD.scala:1137)
	org.apache.spark.rdd.ParallelCollectionRDD.<init>(ParallelCollectionRDD.scala:85)
	org.apache.spark.SparkContext.parallelize(SparkContext.scala:436)
	StageNameTest$.main(StageNameTest.scala:11)  //由于代码没贴全，所以linenumber=11是我代码中的位置
	StageNameTest.main(StageNameTest.scala)
```
调用栈由内向外查找className满足SPARK_CLASS_REGEX正则的最外层调用，这里就是：
	

	org.apache.spark.SparkContext.parallelize(SparkContext.scala:436)

所以lastSparkMethod=parallelize,  其下一个调用就是firstUserFile, firstUserLine，即firstUserFile=StageNameTest, firstUserLine=11
所以该rdd的creationSiteInfo为 parallelize at StageNameTest:11

#### action/finalStage的callSite
例子中rdd.count()会在runJob中生成callSite，此调用栈为：
```java
	org.apache.spark.util.Utils$.getCallSiteInfo(Utils.scala:817)
	org.apache.spark.SparkContext.getCallSite(SparkContext.scala:1025)
	org.apache.spark.SparkContext.runJob(SparkContext.scala:1044)
	org.apache.spark.SparkContext.runJob(SparkContext.scala:1066)
	org.apache.spark.SparkContext.runJob(SparkContext.scala:1080)
	org.apache.spark.SparkContext.runJob(SparkContext.scala:1094)
	org.apache.spark.rdd.RDD.count(RDD.scala:847)
	StageNameTest$.main(StageNameTest.scala:12)
	StageNameTest.main(StageNameTest.scala)
```
满足要求的最外层调用是 org.apache.spark.rdd.RDD.count(RDD.scala:847)，lastSparkMethod=count, firstUserFile=StageNameTest, firstUserLine=12。
callSite的名称为： count at StageNameTest:12.

### combineByKey
接下来以常见的combineByKey方法生成的stage的名称为例子。
combineByKey中生成了三个RDD(mapSideCombine=true的情况)：MapPartitionsRDD[1]、ShuffledRDD[2]、MapPartitionsRDD[3]。可知MapPartitionsRDD[1]为一个stage。该stage由于不是finalStage，所以其会使用MapPartitionsRDD[1]的creationSiteInfo，即 combineByKey at StageNameTest:12。具体如下所示。

调用栈为：
```java
	org.apache.spark.util.Utils$.getCallSiteInfo(Utils.scala:817)
	org.apache.spark.rdd.RDD.<init>(RDD.scala:1137)
	org.apache.spark.rdd.RDD.<init>(RDD.scala:83)
	org.apache.spark.rdd.MapPartitionsRDD.<init>(MapPartitionsRDD.scala:24)
	org.apache.spark.rdd.RDD.mapPartitionsWithContext(RDD.scala:583)
	org.apache.spark.rdd.PairRDDFunctions.combineByKey(PairRDDFunctions.scala:95)
	org.apache.spark.rdd.PairRDDFunctions.combineByKey(PairRDDFunctions.scala:360)
	StageNameTest$.main(StageNameTest.scala:12)
	StageNameTest.main(StageNameTest.scala)
```
可知最外层调用为org.apache.spark.rdd.PairRDDFunctions.combineByKey(PairRDDFunctions.scala:360)， 所以lastSparkMethod=combineByKey, firstUserFile=StageNameTest, firstUserLine=12。

### AUC计算中的BinaryClassificationMetrics.confusions

