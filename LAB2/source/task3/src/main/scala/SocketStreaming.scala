import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketStreaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount_Characters")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    println("lines="+lines)
    val characters = lines.flatMap(_.toLowerCase.split(" "))
    val pairs = characters.map(letter => (letter.length(), letter))
    val characterCount = pairs.reduceByKey(_ +" , " + _)

    print("Result")
    characterCount.print()
    characterCount.saveAsTextFiles("/Users/madhurisarode/Documents/BDP lessons/Workspace/Lab2/OutputCleanedTweets/WordCount.txt")
    ssc.start()
    ssc.awaitTermination()
  }

}
