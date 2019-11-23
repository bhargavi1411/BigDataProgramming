import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._

object ICP5 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setMaster("local[2]").setAppName("Graph")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Graphs")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val trips_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("201508_trip_data.csv")

    val station_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("201508_station_data.csv")

    // Printing the Schema
    trips_df.printSchema()
    station_df.printSchema()


    //First of all create three Temp View
    trips_df.createOrReplaceTempView("Trips")
    station_df.createOrReplaceTempView("Stations")

    station_df.select("dockcount").distinct().show()

    val nstation = spark.sql("select * from Stations")

    val ntrips = spark.sql("select * from Trips")

    val stationVertices = nstation
      .withColumnRenamed("name", "id")
      .distinct()

    val tripEdges = ntrips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")

    //concatenation
    val stationGraph = GraphFrame(stationVertices, tripEdges)
    station_df.select(concat(col("lat"),lit(" "),col("long"))).alias("location").show(10)


    tripEdges.cache()
    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + ntrips.count)

    stationGraph.vertices.show()
    stationGraph.edges.show()

    val inDeg = stationGraph.inDegrees
    println("InDegree" + inDeg.orderBy(desc("inDegree")).limit(5))
    inDeg.show(5)

    val outDeg = stationGraph.outDegrees
    println("OutDegree" + outDeg.orderBy(desc("outDegree")).limit(5))
    outDeg.show(5)

    val motifs = stationGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motifs.show()

    val ver = stationGraph.degrees
    ver.show(5)
    println("Degree" + ver.orderBy(desc("Degree")).limit(5))

    stationGraph.vertices.write.csv("/Users/josmithatammareddy/IdeaProjects/Spark_ICP_5/Graphs1/Vertices")
    stationGraph.edges.write.csv("/Users/josmithatammareddy/IdeaProjects/Spark_ICP_5/Graphs1/Edges")

    val heighestdestination = stationGraph
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    heighestdestination.show(10)

    val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
      .drop(outDeg.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")

    degreeRatio.cache()
    degreeRatio.orderBy(desc("degreeRatio")).show(10)

  }
}


