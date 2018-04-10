/**
  * Created by fz56 on 11/21/2016.
  */
package msu.dasi.SamplingMethods

import org.apache.spark.graphx._
import scala.collection.mutable.{ListBuffer, Set}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.util.Random

object SamplingMethods {

  //Random Node Sampling Method
  def randomNode(sc: SparkContext,
                 percentage: Double,
                 graph: Graph[Int, Int]): Graph[Int, Int] = {

    val alpha = sc.broadcast(percentage)
    var sampledGraph = graph
      .mapVertices { case (id, value) => (value, Random.nextDouble) }
      .cache()

    sampledGraph = sampledGraph
      .subgraph(vpred = (id, attr) => attr._2 <= alpha.value)
      .cache()

    val result =
      sampledGraph.mapVertices { case (id, value) => value._1 }.cache()

    result

  }

  //Random Node-Edge Sampling Method
  def randomNodeEdge(sc: SparkContext,
                     percentage: Double,
                     graph: Graph[Int, Int]): Graph[Int, Int] = {

    val alpha = sc.broadcast(percentage)
    val VertexRDD = graph
      .aggregateMessages[Set[Long]](
        triplet => {
          triplet.sendToDst(Set(triplet.srcId))
          triplet.sendToSrc(Set(triplet.dstId))
        },
        _ ++ _
      )
      .cache()

    val edges: RDD[Edge[Int]] = VertexRDD
      .filter { case (id, vd) => Random.nextFloat() <= alpha.value }
      .map {
        case (id, s) => Edge[Int](id, s.toList(Random.nextInt(s.size)), 1)
      }
      .cache()

    VertexRDD.unpersist()
    val result = Graph
      .fromEdges(edges, 1)
      .convertToCanonicalEdges()
      .groupEdges((a, b) => a)
      .subgraph(epred = e => e.srcId != e.dstId)
      .cache()

    result

  }

  //Random Node-Neighbor Sampling Method
  def randomNodeNeighbor(sc: SparkContext,
                         percentage: Double,
                         graph: Graph[Int, Int]): Graph[Int, Int] = {

    val alpha = sc.broadcast(percentage)
    val VertexRDD = graph
      .aggregateMessages[Set[Long]](
        triplet => {
          triplet.sendToDst(Set(triplet.srcId))
          triplet.sendToSrc(Set(triplet.dstId))
        },
        _ ++ _
      )
      .cache()

    val nodesEdges: RDD[Edge[Int]] = VertexRDD
      .filter { case (id, vd) => Random.nextFloat() <= alpha.value }
      .flatMap {
        case (id, e) => for { x <- e.toList } yield Edge[Int](id, x, 1)
      }
      .cache()

    VertexRDD.unpersist()
    val result = Graph
      .fromEdges(nodesEdges, 1)
      .convertToCanonicalEdges()
      .groupEdges((a, b) => a)
      .subgraph(epred = e => e.srcId != e.dstId)
      .cache()

    result

  }

  //Random Edge Sampling Method
  def randomEdge(sc: SparkContext,
                 percentage: Double,
                 graph: Graph[Int, Int]): Graph[Int, Int] = {

    val alpha = sc.broadcast(percentage)
    val edges: RDD[Edge[Int]] = graph
      .subgraph(epred = e => Random.nextDouble <= alpha.value)
      .edges
      .cache()

    val result = Graph.fromEdges(edges, 1).cache()

    result
  }

  //Random Hybird Sampling Method
  def randomHybrid(sc: SparkContext,
                   percentage: Double,
                   graph: Graph[Int, Int]): Graph[Int, Int] = {

    val alpha = sc.broadcast(percentage)
    val p = 0.8

    val randomNodeGraph = randomNodeEdge(sc, p * alpha.value, graph).cache()
    val randomEdgeGraph = randomEdge(sc, (1 - p) * alpha.value, graph).cache()

    var result = Graph(randomNodeGraph.vertices.union(randomEdgeGraph.vertices),
                       randomNodeGraph.edges.union(randomEdgeGraph.edges))
      .partitionBy(PartitionStrategy.EdgePartition2D)

    result = result
      .groupEdges((a, b) => a)
      .subgraph(epred = e => e.srcId != e.dstId)
      .cache()

    result
  }

  //Random Total Induced Edge Sampling Method
  def totalInducedEdge(sc: SparkContext,
                       percentage: Double,
                       graph: Graph[Int, Int]): Graph[Int, Int] = {

    val alpha = sc.broadcast(percentage)
    val inducedGraph = graph.mapEdges(e => Random.nextDouble).cache()

    val edges =
      inducedGraph.subgraph(epred = e => e.attr <= alpha.value).edges.cache()

    val tempGraph = Graph.fromEdges(edges, 1).cache()

    val joinedGraph = graph
      .outerJoinVertices(tempGraph.vertices)((vid, data, attr) => {
        attr.getOrElse(0)
      })
      .cache()

    val newGraph = joinedGraph
      .mapTriplets(triplet => {
        if (triplet.dstAttr != 0 || triplet.srcAttr != 0) {
          0
        } else 1
      })
      .cache()

    tempGraph.unpersist()
    joinedGraph.unpersist()
    val newEdges = newGraph.edges.filter(e => e.attr == 0).cache()
    val result = Graph.fromEdges(newEdges, 1).mapEdges(e => 1).cache()

    result
  }

  //breadth First Sampling Method
  def breadthFirst(sc: SparkContext,
                   percentage: Double,
                   diameter: Int,
                   graph: Graph[Int, Int]): Graph[Int, Int] = {

    val k = sc.broadcast((graph.numVertices * percentage).toInt)
    var initialGraph = graph.mapVertices((id, _) => Int.MaxValue).cache()
    val ccGraph = graph.connectedComponents().cache()

    val componentLists =
      ccGraph.vertices.map { case (id, cc) => cc }.collect().distinct

    val componentIndexList = new ListBuffer[VertexId]
    componentLists.foreach(componentIndexList.+=:)

    val indexSeq = Random.shuffle(componentIndexList)
    var sumVertices: Int = 0

    for (index <- indexSeq if sumVertices < k.value) {

      val indexList = new ListBuffer[VertexId]
      val temp = ccGraph.vertices.filter { case (id, attr) => attr == index }
      temp.map { case (id, attr) => id }.collect.foreach(indexList.+=:)
      val randomNodeID = indexList(Random.nextInt(temp.collect.indices.size))

      initialGraph = initialGraph.mapVertices((id, attr) =>
        if (id == randomNodeID) sumVertices else attr)

      initialGraph = initialGraph.pregel(Int.MaxValue, diameter)(
        (id, attr, msg) => math.min(attr, msg),
        triplet => {
          if (triplet.srcAttr != Int.MaxValue) {
            Iterator((triplet.dstId, triplet.srcAttr + 1))
          } else if (triplet.dstAttr != Int.MaxValue) {
            Iterator((triplet.srcId, triplet.dstAttr + 1))
          } else {
            Iterator.empty
          }
        },
        (a, b) => math.min(a, b)
      )

      sumVertices += ccGraph.vertices
        .filter { case (id, attr) => attr == index }
        .count()
        .toInt
    }

    val vertexIndexList = initialGraph.vertices
      .filter { case (id, attr) => attr != Int.MaxValue }
      .map { case (id, cc) => cc }
      .collect()
      .distinct
      .sorted

    var sumNodes = 0
    var lastIndex = vertexIndexList(0)
    var lastSumVertices = 0

    for (index <- vertexIndexList if sumNodes < k.value) {
      lastSumVertices = sumNodes
      sumNodes += initialGraph.vertices
        .filter { case (id, attr) => attr == index }
        .count
        .toInt
      lastIndex = index
    }

    val percent = (k.value - lastSumVertices).toDouble / initialGraph.vertices
      .filter { case (id, attr) => attr == lastIndex }
      .count
      .toDouble

    val tempGraph = initialGraph
      .mapVertices { case (id, attr) => (attr, Random.nextFloat()) }
      .cache()

    val resultGraph = tempGraph
      .subgraph(vpred = (id, attr) =>
        (attr._1 < lastIndex) || (attr._1 == lastIndex && attr._2 < percent))
      .mapVertices { case (id, value) => 1 }
      .cache()

    resultGraph
  }

  //Forest Fire Sampling Method
  def forestFire(sc: SparkContext,
                 percentage: Double,
                 diameter: Int,
                 graph: Graph[Int, Int]): Graph[Int, Int] = {

    val k = sc.broadcast((graph.numVertices * percentage).toInt)
    var initialGraph = graph.mapVertices((id, _) => Int.MaxValue).cache()
    val ccGraph = graph.connectedComponents().cache()

    val componentLists =
      ccGraph.vertices.map { case (id, cc) => cc }.collect().distinct
    val componentIndexList = new ListBuffer[VertexId]
    componentLists.foreach(componentIndexList.+=:)

    val indexSeq = Random.shuffle(componentIndexList)
    var sumVertices: Int = 0

    for (index <- indexSeq if sumVertices < k.value) {

      val indexList = new ListBuffer[VertexId]
      val temp = ccGraph.vertices.filter { case (id, attr) => attr == index }
      temp.map { case (id, attr) => id }.collect.foreach(indexList.+=:)

      val randomNodeID = indexList(Random.nextInt(temp.collect.indices.size))
      initialGraph = initialGraph.mapVertices((id, attr) =>
        if (id == randomNodeID) sumVertices else attr)

      initialGraph = initialGraph.pregel(Int.MaxValue, diameter)(
        (id, attr, msg) => math.min(attr, msg),
        triplet => {
          if (triplet.srcAttr != Int.MaxValue && Random.nextDouble <= 0.7) {
            Iterator((triplet.dstId, triplet.srcAttr + 1))
          } else if (triplet.dstAttr != Int.MaxValue && Random.nextDouble <= 0.7) {
            Iterator((triplet.srcId, triplet.dstAttr + 1))
          } else {
            Iterator.empty
          }
        },
        (a, b) => math.min(a, b)
      )

      sumVertices += ccGraph.vertices
        .filter { case (id, attr) => attr == index }
        .count()
        .toInt
    }

    val vertexIndexList = initialGraph.vertices
      .filter { case (id, attr) => attr != Int.MaxValue }
      .map { case (id, cc) => cc }
      .collect()
      .distinct
      .sorted

    var sumNodes = 0
    var lastIndex = vertexIndexList(0)
    var lastSumVertices = 0

    for (index <- vertexIndexList if sumNodes < k.value) {

      lastSumVertices = sumNodes
      sumNodes += initialGraph.vertices
        .filter { case (id, attr) => attr == index }
        .count
        .toInt
      lastIndex = index

    }

    val percent = (k.value - lastSumVertices).toDouble / initialGraph.vertices
      .filter { case (id, attr) => attr == lastIndex }
      .count
      .toDouble

    val tempGraph = initialGraph
      .mapVertices { case (id, attr) => (attr, Random.nextFloat()) }
      .cache()

    val resultGraph = tempGraph
      .subgraph(vpred = (id, attr) =>
        (attr._1 < lastIndex) || (attr._1 == lastIndex && attr._2 < percent))
      .mapVertices { case (id, value) => 1 }
      .cache()

    resultGraph

  }

  //Snow Ball Sampling Method
  def snowball(sc: SparkContext,
               percentage: Double,
               diameter: Int,
               graph: Graph[Int, Int]): Graph[Int, Int] = {

    val k = sc.broadcast((graph.numVertices * percentage).toInt)
    var initialGraph = graph.mapVertices((id, _) => Int.MaxValue).cache()
    val ccGraph = graph.connectedComponents().cache()

    val componentLists =
      ccGraph.vertices.map { case (id, cc) => cc }.collect().distinct

    val componentIndexList = new ListBuffer[VertexId]
    componentLists.foreach(componentIndexList.+=:)
    val indexSeq = Random.shuffle(componentIndexList)

    var sumVertices: Int = 0
    for (index <- indexSeq if sumVertices < k.value) {

      val indexList = new ListBuffer[VertexId]
      val temp = ccGraph.vertices.filter { case (id, attr) => attr == index }
      temp.map { case (id, attr) => id }.collect.foreach(indexList.+=:)
      val randomNodeID = indexList(Random.nextInt(temp.collect.indices.size))

      initialGraph = initialGraph.mapVertices((id, attr) =>
        if (id == randomNodeID) sumVertices else attr)

      initialGraph = initialGraph.pregel(Int.MaxValue, diameter)(
        (id, attr, msg) => math.min(attr, msg),
        triplet => {
          if (triplet.srcAttr != Int.MaxValue && Random.nextDouble <= 0.5) {
            Iterator((triplet.dstId, triplet.srcAttr + 1))
          } else if (triplet.dstAttr != Int.MaxValue && Random.nextDouble <= 0.5) {
            Iterator((triplet.srcId, triplet.dstAttr + 1))
          } else {
            Iterator.empty
          }
        },
        (a, b) => math.min(a, b)
      )

      sumVertices += ccGraph.vertices
        .filter { case (id, attr) => attr == index }
        .count()
        .toInt

    }

    val vertexIndexList = initialGraph.vertices
      .filter { case (id, attr) => attr != Int.MaxValue }
      .map { case (id, cc) => cc }
      .collect()
      .distinct
      .sorted

    var sumNodes = 0
    var lastIndex = vertexIndexList(0)
    var lastSumVertices = 0

    for (index <- vertexIndexList if sumNodes < k.value) {

      lastSumVertices = sumNodes
      sumNodes += initialGraph.vertices
        .filter { case (id, attr) => attr == index }
        .count
        .toInt
      lastIndex = index
    }

    val percent = (k.value - lastSumVertices).toDouble / initialGraph.vertices
      .filter { case (id, attr) => attr == lastIndex }
      .count
      .toDouble

    val tempGraph = initialGraph
      .mapVertices { case (id, attr) => (attr, Random.nextFloat()) }
      .cache()

    val resultGraph = tempGraph
      .subgraph(vpred = (id, attr) =>
        (attr._1 < lastIndex) || (attr._1 == lastIndex && attr._2 < percent))
      .mapVertices { case (id, value) => 1 }
      .cache()

    resultGraph

  }

}
