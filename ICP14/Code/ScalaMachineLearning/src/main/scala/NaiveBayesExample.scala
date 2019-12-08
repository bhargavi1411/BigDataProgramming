// scalastyle:off println
//package org.apache.spark.examples.mllib
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

// $example off$

object NaiveBayesExample {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    //val conf = new SparkConf().setAppName("NaiveBayesExample")
    // val sc = new SparkContext(conf)
    val conf = new SparkConf().setAppName("Samples").setMaster("local")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "/Users/madhurisarode/Downloads/ScalaMachineLearning/src/main/scala/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    // Save and load model
    model.save(sc, "target/tmp/myNaiveBayesModel")
    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
    // $example off$



    println("Accuracy="+accuracy)
    val metrics = new BinaryClassificationMetrics(predictionAndLabel)
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
    val metrics1 = new MulticlassMetrics(predictionAndLabel)
    // Confusion matrix
    println("Confusion matrix:"+metrics1.confusionMatrix)


    sc.stop()
  }
}

// scalastyle:on println