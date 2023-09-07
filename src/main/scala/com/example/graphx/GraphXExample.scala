import org.apache.spark.graphx.{Graph, VertexRDD,Edge}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import scala.util.Random
import org.apache.spark.util.SizeEstimator


object GraphXExample {
  type PID = Long
  type HID = Long
  type VID = Long
  type VAL = Long
  // type HAttr = (Int, Int)  // (Cardinality, weight=1)
  // type VAttr = Unit
  // type Structure = RDD[(HID, VID)]
  // type HAttrs = RDD[(HID, HAttr)]
  // type VAttrs = RDD[(VID, VAttr)]


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphXExample")
    val sc = new SparkContext(conf)
    // val filename = "/data/github16/mesh-random.txt" 
    val vFile = "/data/mesh_input2/mesh-random-vert.txt"
    val eFile = "/data/mesh_input2/mesh-random-edge.txt"
    val numPartitions : Int = 4

    // 获取一个Logger实例，通常使用类名作为Logger名称
    val logger: Logger = Logger.getLogger(getClass.getName)
    // 设置日志级别
    logger.setLevel(Level.INFO)

    // val structure: RDD[(HID,VID)] = sc.textFile(filename).map { line =>
    //   val Array(hid, vid) = line.split(",").map(_.toLong)
    //   hid -> vid
    // }
    val vInfo: RDD[(VID,PID)] = sc.textFile(vFile).map { line =>
      val Array(vid, pid) = line.split(",").map(_.toLong)
      vid -> pid
    }
    val eInfo: RDD[((VID,VID),PID)] = sc.textFile(eFile).map { line =>
      val Array(hid, vid, pid) = line.split(",").map(_.toLong)
      ((hid,vid),pid)
    }
    // structure.saveAsTextFile("file:///home/graphx-example/ghh-structure")
    // val heID:RDD[HID] = structure.map{case(hid,vid) => hid}.distinct
    // val hvID:RDD[VID] = structure.map{case(hid,vid) => vid}.distinct

    // val random = new Random()
    // val hvPartList:List[(VID,PID)] = hvID.map{case nid => (nid,nid%numPartitions)}.collect().toList
    // val hvPartList: RDD[(VID, PID)] = hvID.mapPartitions { partition =>
    //   val random = new Random()
    //   partition.map { nid =>
    //     (nid, random.nextInt() % numPartitions)
    //   }
    // }
    // val hvPartDictBuf:Map[VID,PID] = hvPartList.toMap // VID -> PID to check hyperVertex's partition 
    // val hvPartDict = sc.broadcast(hvPartDictBuf).value

    // val maxHyperVertexId = hvID.max
    // val maxHyperEdgeId = heID.max
    // val shift = maxHyperVertexId + 1

    // heID.saveAsTextFile("file:///home/graphx-example/ghh-heID")
    // hvID.saveAsTextFile("file:///home/graphx-example/ghh-hvID")
    // val hePartList:List[(HID,PID)] = structure.map{case(hid,vid) => (hid,hvPartDict.getOrElse(vid,-1):PID)}.collect().toList.distinct.sorted
    // var cnt:VID = shift
    // val hePartDistBuf:Map[(HID,PID),VID] = hePartList.map{case(hid,pid) => {cnt += 1; (hid,pid) -> cnt} }.toMap //(HID,PID) -> VID , base (HID,PID) to check hyperedge copy's VID
    // val hePartDist = sc.broadcast(hePartDistBuf).value

    // val tmp = sc.parallelize(for(i <- maxHyperVertexId + 1 until cnt + 1 ) yield(i,0L))
    // val broadcastSize = SizeEstimator.estimate(tmp)
    // logger.info(s"tmp size ghh:$broadcastSize")

    // val vAttr:RDD[(VID,VAL)] = hvID.map{case vid => (vid, 0L)} // union tmp
    // val broadcastSizeVAttr = SizeEstimator.estimate( vAttr)
    // logger.info(s"tmp size vAttr ghh:$broadcastSizeVAttr")
    // val innerEdge = hePartList.groupBy(_._1).flatMap {
    //   case (hid, pidList) =>
    //     val combinations = for {
    //       x <- pidList
    //       y <- pidList
    //       if x != y  // 避免将元素与自身组合
    //     } yield {
    //       (hePartDist.getOrElse(x, -1), hePartDist.getOrElse(y, -1))
    //     }
    //     combinations
    // }.toList

    // val innerEdgeRDD:RDD[Edge[VAL]] =  sc.parallelize(innerEdge.map{
    //   case (srcId:VID,tarId:VID) => Edge(srcId,tarId,0L)
    // })
    // val eAttr:RDD[Edge[Long]] = innerEdgeRDD
    // val eAttr:RDD[Edge[Long]] = structure.map{case (hid,vid) => {
    //     val pid:PID = hvPartDict.getOrElse(vid,-1)
    //     val heid:VID =  hePartDist.getOrElse((hid,pid),-1)
    //     Edge(vid, heid, 0L)
    //   }
    // } union structure.map{case (hid,vid) => {
    //     val pid:PID = hvPartDict.getOrElse(vid,-1)
    //     val heid:VID =  hePartDist.getOrElse((hid,pid),-1)
    //     Edge(heid, vid, 0L)
    //   }
    // } union innerEdgeRDD
    // // vAttr.saveAsTextFile("file:///home/graphx-example/ghh-vAttr")
    val vAttr:RDD[(VID,(PID,VAL))] = vInfo.map{case (vid,pid) => (vid, (pid,0L))}
    val eAttr:RDD[Edge[(PID,VAL)]] = eInfo.map{case ((srcId,dstId),pid) => Edge(srcId, dstId, (pid,0L))}
    val shift = eInfo.map { case ((hid, _), _) => hid }.reduce(math.min)

    val graph = Graph(vAttr, eAttr)
    // val vDict = hvPartDict ++ hePartDist.map{ case ((hid,pid),vid) => vid -> pid} 

    val graphV = graph.numVertices
    val graphE = graph.numEdges

    // logger.info(s"Vertex num:$maxHyperVertexId Edge num:$maxHyperEdgeId")
    logger.info(s"graph Vertices number:$graphV Edge number:$graphE")

    // val subGraphAgg = for (i <- 0 until numPartitions) yield {
    //   val subgraph = graph.subgraph(
    //       vpred = (id, attr) => vDict.getOrElse(id,-1) == i, 
    //       epred = edge => vDict.getOrElse(edge.srcId,-1) == i 
    //   )
    //   val vertexCount = subgraph.numVertices
    //   val edgeCount   = subgraph.numEdges
    //   // logger.info(s"ghh:Aggerate subgraph partition$i vertexCount:$vertexCount edgeCount:$edgeCount")
    //   (i,subgraph)
    // }

    // val subGraphAgg = for (i <- 0 until 4) yield {
    //       val subgraph = graph.subgraph(
    //           vpred = (id, attr) => vDict.getOrElse(id,-1) == 0, 
    //           epred = edge => vDict.getOrElse(edge.srcId,-1) == 0
    //       )
    //       subgraph
    // }
    // subGraphAgg.foreach{
    //   case subGraph => {
    //     // logger.info(s"pid$pid")
    //     val vertexCount = subGraph.numVertices
    //     val edgeCount   = subGraph.numEdges
    //     logger.info(s"ghh:Aggerate subgraph partition vertexCount:$vertexCount edgeCount:$edgeCount")
    //   }
    // }
    // val subgraph = graph.subgraph(
    //       vpred = (id, attr) => id >= shift, 
    //       epred = edge => edge.srcId >= shift && edge.dstId >= shift 
    // )
    // val vertexCount = subgraph.numVertices
    // val edgeCount   = subgraph.numEdges

    val subGraphAgg = graph.subgraph(
          vpred = (id, attr) => true, 
          epred = edge => edge.dstId >= shift 
      )
    val subGraphCom = graph.subgraph(
          vpred = (id, attr) => id >= shift, 
          epred = edge => edge.srcId >= shift && edge.dstId >= shift 
      )

    val subGraphBak = graph.subgraph(
          vpred = (id, attr) => true, 
          epred = edge => edge.dstId < shift 
      )


    // val subGraphBak = for (i <- 0 until numPartitions) yield {
    //   val subgraph = graph.subgraph(
    //       vpred = (id, attr) => vDict.getOrElse(id,-1) == i, 
    //       epred = edge => vDict.getOrElse(edge.dstId,-1) == i 
    //   )
    //   (i,subgraph)
    // }
    // subGraphAgg.foreach{
    //   case (pid,subGraph) => {
    //     logger.info(s"pid$pid")
    //     // val vertexCount = subGraph.numVertices
    //     // val edgeCount   = subGraph.numEdges
    //     // logger.info(s"ghh:Aggerate subgraph partition$pid vertexCount:$vertexCount edgeCount:$edgeCount")
    //   }
    // }

    // subGraphCom.foreach{
    //   case (pid,subGraph) => {
    //     // val vertexCount = subGraph.numVertices
    //     // val edgeCount   = subGraph.numEdges
    //     // logger.info(s"ghh:Communication subgraph partition$pid vertexCount:$vertexCount edgeCount:$edgeCount")
    //   }
    // }

    // subGraphBak.foreach{
    //   case (pid,subGraph) => {
    //     val vertexCount = subGraph.numVertices
    //     val edgeCount   = subGraph.numEdges
    //     logger.info(s"ghh:sendbak subgraph partition$pid vertexCount:$vertexCount edgeCount:$edgeCount")
    //   }
    // }

    // val subGraphAgg = graph.subgraph(vpred = (id, attr) => id < shift)
    // val subGraphCom = graph.subgraph(vpred = (id, attr) => id >= shift)
    // val subGraphBak = 
    // val vAttrPartitionNum = vAttr.getNumPartitions
    // val eAttrPartitionNum = eAttr.getNumPartitions
    // logger.info(s"Vertex num:$maxHyperVertexId Edge num:$maxHyperEdgeId")
    // logger.info(s"vAttr partition num:$vAttrPartitionNum eAttr patition num:$eAttrPartitionNum")

    // val customPartitioner = new CustomPartitioner(numPartitions, partDict)
    // val repartHvGraph: RDD[(Long, Long)] = hvGraph.partitionBy(customPartitioner)




    sc.stop()
  }

//   class CustomPartitioner(numPartition: Int, dict: Map[Long,Int]) extends Partitioner {
//   // 指定分区数
//   override def numPartitions: Int = numPartition

//   // 自定义分区逻辑，根据元素的某些属性来分区
//   override def getPartition(key: Any): Int = key match {
//     case vid: VID =>
//       // 根据顶点的 ID 进行分区，示例中简单地取余操作
//       dict.getOrElse(vid,-1)

//     case triplet: EdgeTriplet[_, _] =>
//       // 根据边的信息进行分区，示例中简单地取余操作
//       triplet.srcId.toInt % numPartitions

//     case _ =>
//       throw new IllegalArgumentException("Unsupported key type")

//     case nid: Long =>
//       dict.getOrElse(nid, -1) // 使用getOrElse处理不存在的键
//     case _ =>
//       -1 // 如果 key 不是 Long 类型，返回 -1 表示无效分区
//   }
// }
}

