package com.demo

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Nashville {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    val conf = new SparkConf().setMaster("local[2]").setAppName("PAGE_RANK")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("PAGE_RANK")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val groups_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/bhargavichennupati/Downloads/untitled5/meta-groups.csv")

    val edges_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/bhargavichennupati/Downloads/untitled5/group-edges.csv")



    // Printing the Schema

    edges_df.printSchema()

    groups_df.printSchema()


    edges_df.createOrReplaceTempView("e")

    groups_df.createOrReplaceTempView("g")


    val g2 = spark.sql("select * from g")

    val e2 = spark.sql("select * from e")

    val vertices = g2
      .withColumnRenamed("group_id", "id").limit(200)
      .distinct()

    val edges = e2
      .withColumnRenamed("group1", "src").limit(600).distinct()
      .withColumnRenamed("group2", "dst").limit(600).distinct()


    val graph = GraphFrame(vertices, edges)

    edges.cache()
    vertices.cache()
    graph.vertices.show()
    graph.edges.show()


    println("Total Number of vertices: " + graph.vertices.count)
    println("Total Number of edges: " + graph.edges.count)


    val stationPageRank = graph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.show()
    stationPageRank.edges.show()
  }

}