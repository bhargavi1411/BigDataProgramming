import org.apache.spark.{SparkConf, SparkContext}

object ReadResult {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Samples").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet("/Users/madhurisarode/Downloads/ScalaMachineLearning/target/tmp/myNaiveBayesModel/data/part-00000-6bafc8fc-0454-449c-b581-94673a82d127.snappy.parquet")

    df.printSchema
    df.show()

  }
}
