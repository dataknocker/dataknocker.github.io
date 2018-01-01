title: spark stage shuffle重用
date: 2014-11-12 15:12:38
categories: spark
tags: [spark, shuffle]
---
在运行时看DAG 图时有时候会现一些stage会被跳过。
这是因为当一个经过shuffle后的rdd，如ShuffledRDD, 再次被使用时，该ShuffledRDD对应的stage不会重新计算(除非某些分区数据出问题了，也只会单独对这些分区进行计算)。
具体原理：

1、ShuffledRDD的ShuffleDependency中的shuffleId对于这个shuffledRDD初始化是不变的。
2、shuffleToMapStage保存了之前的shuffleId与相应的stage
DAGScheduler:
getMissingParentStages:
	getShuffleMapStage(shufDep, stage.jobId):

	shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None =>
        // We are going to register ancestor shuffle dependencies
        registerShuffleDependencies(shuffleDep, jobId)
        // Then register current shuffleDep
        val stage =
          newOrUsedStage(
            shuffleDep.rdd, shuffleDep.rdd.partitions.size, shuffleDep, jobId,
            shuffleDep.rdd.creationSite)
        shuffleToMapStage(shuffleDep.shuffleId) = stage
 
        stage
    }

3、某个shufflerdd完成后，第二次被调用时，shuffleDep.shuffleId就是和之前一样的，所以直接返回之前生成的stage.

4、而在DAGScheduler的submitMissingTasks中

	val partitionsToCompute: Seq[Int] = {
      if (stage.isShuffleMap) {
        (0 until stage.numPartitions).filter(id => stage.outputLocs(id) == Nil)
      }
    }

partitionsToCompute是作为真正要跑的task来源，发现只有stage.outputLocs(id) == Nil才会被放到task列表中。

可见由于stage是用之前的，其outputLocs在第一次shuffle完就有值了，所以这时就不会再对这些task进行计算了。

