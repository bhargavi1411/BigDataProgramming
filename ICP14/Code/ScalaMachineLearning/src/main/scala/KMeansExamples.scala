// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
// $example off$

object KMeansExamples {

  def main(args: Array[String]): Unit = {



    val conf = new SparkConf().setAppName("Samples").setMaster("local")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
    val data = sc.textFile("/Users/madhurisarode/Downloads/ScalaMachineLearning/Input/diabetic_data_new.csv")
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Save and load model
    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    // $example off$

    println("Cluster centers data")
     for (n <- sameModel.clusterCenters)
       {
         println(n)
       }


    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet("/Users/madhurisarode/Downloads/ScalaMachineLearning/target/org/apache/spark/KMeansExample/KMeansModel/data")

    df.printSchema
    sc.stop()
  }
}
// scalastyle:on println