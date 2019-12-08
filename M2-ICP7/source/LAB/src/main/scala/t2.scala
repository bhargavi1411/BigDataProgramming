import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}

object t2 {

  def main(args: Array[String]): Unit = {

    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("Task2")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Task2")
      .config(conf =conf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // We are using all 3 Fifa dataset given on Kaggle Repository
    //a.Import the dataset and create df and print Schema

    val df1 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/bhargavichennupati/Downloads/LAB/WorldCups.csv")

    val df2 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/bhargavichennupati/Downloads/LAB/WorldCupPlayers.csv")

    val df3 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/bhargavichennupati/Downloads/LAB/WorldCupMatches.csv")

    // Printing the Schema
    df1.printSchema()
    df2.printSchema()
    df3.printSchema()

    //b.Perform   10   intuitive   questions   in   Dataset
    //For this problem we have used the Spark SqL on DataFrames

    //First of all create three Temp View
    df1.createOrReplaceTempView("WC")
    df2.createOrReplaceTempView("Players")
    df3.createOrReplaceTempView("Matches")


    // Find the winner by years using WorldCup view
    val Q = spark.sql("select Winner, Country, Year from WC Order By Country ")
    Q.show()

    //Find the goals by years using WorldCup view
    val Q1 = spark.sql("select QualifiedTeams, MatchesPlayed, Year from WC WHERE Country = 'Brazil' Order By Year")
    Q1.show()

    //Cities that hosted highest world cup matches on view wcMatches
    val Q2 = spark.sql("select Count(City),City from Matches Group By City")
    Q2.show()

    //Teams with the most world cup final victories on WorldCup view
    val Q3 = spark.sql("select Count(Winner),Winner,Attendance from WC Group By Winner, Attendance")
    Q3.show()

    // Display all Stage Finalers in the year 1934
    val Q4 = spark.sql("select * from Matches where Stage = 'Final' AND Year  = 1934 ")
    Q4.show()

    //matches held by coach CAUDRON Raoul (FRA)
    val Q5 =spark.sql("select * from Players where `Coach Name` = 'CAUDRON Raoul (FRA)'")
    Q5.show()

    //No of matches in year 1934 and in san siro stadium
    val Q6 = spark.sql("select count(*) from Matches where year=1934 AND Stadium = 'San Siro' ")
    Q6.show()

    //number of matches that held in Estadio Centenario stadium
    val Q7 = spark.sql("select count(*) from Matches where Stadium = 'Estadio Centenario'")
    Q7.show()

    //Country which hoster World Cup highest number of times
    val Q8 = spark.sql("select Count(Country),Country,Year from WC Group by Country,Year")
    Q8.show()

    //Stadium with highest number of matches
    val Q9 = spark.sql("select Count(Stadium),Stadium from Matches Group By Stadium")
    Q9.show()

    val Q10 = spark.sql("select `Player Name`, Position from Players where Position = 'GK' ")
    Q10.show()



    //HomeTeam Goals Count and their stage by Years
    val Q11 = spark.sql("select `Home Team Name`,Stage,Year FROM Matches Group By Year,`Home Team Name`,Stage")
    Q11.show()

    // Away Team Goals and their stage
    val Q12 = spark.sql("select `Away Team Name`,Stage,Year from Matches Group By Year,`Away Team Name`,Stage")
    Q12.show()


    //Perform any 5 queries in Spark RDD’s and Spark Data Frames.
    // To Solve this Problem we first create the rdd as we already have Dataframe df1 created above code
    // RDD creation

    val csv = sc.textFile("/Users/bhargavichennupati/Downloads/LAB/WorldCups.csv")

    val h1 = csv.first()

    val data = csv.filter(line => line != h1)

    data.foreach(println)

    val rdd = data.map(line=>line.split(",")).collect()

    //rdd.foreach(println)
    //RDD Highest Numbers of goals
    val rdd1 = data.filter(line => line.split(",")(0) == "2006").map(line => (line.split(",")(0),
      (line.split(",")(1)), (line.split(",")(2)), (line.split(",")(3)) ) )
    rdd1.foreach(println)

    // Dataframe
    df1.select("Year","Country", "Winner").filter("Year =2006").show(10)

    // Dataframe SQL
    val dfQ1 = spark.sql("select Year, Country, Winner FROM WC WHERE Year = 2006 order by Year Desc Limit 10").show()

    // Year, Venue country = winning country
    // Using RDD
    val rdd2 = data.filter(line => (line.split(",")(2)=="Italy" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3),line.split(",")(4),line.split(",")(5)
        ,line.split(",")(5))).collect()
    rdd2.foreach(println)

    // Using Dataframe
    df1.select("Year","Winner","Runners-Up","Third", "Fourth").filter("Winner == 'Italy'").show(10)

    // usig Spark SQL
    val DFQ2 = spark.sql("select * from WC where Winner = 'Italy' order by Year").show(10)


    // Details of years ending in ZERO
    // RDD
    val rdd3 = data.filter(line => (line.split(",")(7)>"16" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(6), line.split(",")(7))).collect()
    rdd3.foreach(println)

    //DataFrame
    df1.select("Year","Winner","QualifiedTeams").filter("QualifiedTeams > 16").show(10)

    //DF - SQL
    val DFQ3 = spark.sql("SELECT Year, Winner, QualifiedTeams from WC where QualifiedTeams > 16  ").show(10)

    //2014 world cup stats
    //Rdd
    val rdd4 = data.filter(line => line.split(",")(1)==line.split(",")(5))
      .map(line => (line.split(",")(0),line.split(",")(1), line.split(",")(5)))
      .collect()
    rdd4.foreach(println)

    // Using Dataframe
    df1.select("Year","Country","Fourth").filter("Country==Fourth").show(10)

    // usig Spark SQL
    val DFQ4 = spark.sql("select Year,Country,Fourth from WC where Country = Fourth order by Year").show()

    //Max matches played
    //RDD
    val rdd5 = data.filter(line=>line.split(",")(8) > "55")
      .map(line=> (line.split(",")(0),line.split(",")(8),line.split(",")(3))).collect()
    rdd5.foreach(println)

    // DataFrame
    df1.filter("MatchesPlayed > 55").show()

    // Spark SQL
    val DFQ5 = spark.sql(" Select * from WC where MatchesPlayed in " +
      "(Select Max(MatchesPlayed) from WC )" ).show()

  }

}