// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
// $example off$

object RandomForestClassificationExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Samples").setMaster("local")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "/Users/madhurisarode/Downloads/ScalaMachineLearning/src/main/scala/sample_libsvm_data.txt")

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification forest model:\n ${model.toDebugString}")

    // Save and load model
    model.save(sc, "target/tmp/myRandomForestClassificationModel")
    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
    // $example off$

    val accuracy = 1.0 * labelAndPreds.filter(x => x._1 == x._2).count() / testData.count()
    println("Accuracy = " + accuracy)



    val metrics = new BinaryClassificationMetrics(labelAndPreds)
    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println("Precision by threshold"+s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println("Recall by threshold"+s"Threshold: $t, Recall: $r")
    }

    // Precision-Recall Curve
    val PRC = metrics.pr

    for( m <- PRC)
    {
      println("precision recall curve")
      println(m)
    }




    // Instantiate metrics object
    val metrics1 = new MulticlassMetrics(labelAndPreds)
    // Confusion matrix
    println("Confusion matrix:"+metrics1.confusionMatrix)

    sc.stop()
  }
}
// scalastyle:on println