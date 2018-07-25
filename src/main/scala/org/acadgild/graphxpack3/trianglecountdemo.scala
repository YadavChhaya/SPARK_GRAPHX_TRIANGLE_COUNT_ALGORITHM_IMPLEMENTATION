package org.acadgild.graphxpack3

/**
 *  GRAPHX TRIANGLE COUNT ALGORITHM
 *  Load the edges in canonical order and partition the graph for triangle count.
 *  Partition strategy is used internally by spark to do distributed computing
 *  CanonicalOrientation - whether to orient edges in the positive direction
 *
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object trianglecountdemo {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("GraphTriangleCountAlgo")

    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, "/home/acadgild/follower_demo.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)

    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices

    triCounts.collect()

    // Join the triangle counts with the usernames
    
    val users = sc.textFile("/home/acadgild/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    users.collect()

    val triCountByUsername = users.join(triCounts).map {
      case (id, (username, tc)) =>
        (username, tc)
    }

    // Print the result
    println(triCountByUsername.collect().mkString("\n"))
    
    sc.stop()

  }

}