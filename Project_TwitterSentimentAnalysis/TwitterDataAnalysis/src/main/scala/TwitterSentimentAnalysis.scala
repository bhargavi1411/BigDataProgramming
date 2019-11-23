import java.io.{BufferedWriter, File, FileOutputStream, FileWriter, PrintWriter}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source


/**
 * Use case : Research about success of movies in different categories. This gives the film makers a idea about what genre movies are popular and liked by audience.
 * This also gives commercial advantage, for investment.
 */
object TwitterSentimentAnalysis{
  def main(args: Array[String]) {

    //Check Spark job at http://192.168.1.7:4040/jobs/
    System.setProperty("hadoop.home.dir", "/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis")
    val conf = new SparkConf().setAppName("TwitterDataAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)


    def writeToFile(data: String,file: File)  = {
      val fileObject = new PrintWriter(new FileOutputStream(file, true))
      val printWriter = new PrintWriter(fileObject)
      printWriter.write(data+"\n")
      printWriter.close()
    }


    //Clean output directory for new results and set up directory structure for saving the results
    FileUtils.cleanDirectory(new File("Output"))
    FileUtils.cleanDirectory(new File("Result"))
    FileUtils.cleanDirectory(new File("Conclusion"))
    val myDir1 = new File("Output/CleanedTweets")
    myDir1.mkdir()
    val myDir2 = new File("Output/HashTagListFromTweets")
    myDir2.mkdir()
    val myDir3 = new File("Conclusion")
    myDir3.mkdir()
    val GenrePopularityFile = "/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Conclusion/GenrePopularity.txt"
    val fileGenrePopularity = new File(GenrePopularityFile)
    fileGenrePopularity.createNewFile()
    val FinalConclusionFilecsv = "/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Conclusion/ConclusionData.csv"
    val fileConclusioncsv = new File(FinalConclusionFilecsv)
    fileConclusioncsv.createNewFile()
    val header ="Genre,MovieName,positive_tweet_count,negetive_tweet_count,neutral_tweet_count,total_tweet_count"
    writeToFile(header,fileConclusioncsv)


    //Extract the file names to be processed from input file directory
    def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
      dir.listFiles.filter(_.isFile).toList.filter { file =>
        extensions.exists(file.getName.endsWith(_))
      }
    }


    val inputFilePath = "/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Input"
    val okFileExtensions = List( "txt")
    val moviesList = (getListOfFiles(new File(inputFilePath), okFileExtensions))



    //Read all the movies trailer tweets files from the list
  //  val moviesList = Set("AladdinTrailer.txt","AngryBirds2Trailer.txt","Frozen2Trailer.txt","LionKingTrailer.txt","Minions2.txt","ToyStory4Trailer.txt")


    for(eachMovieFileName <- moviesList)
      {
        val movieName = eachMovieFileName.toString().substring(inputFilePath.length+1)
        readFileAndCleanTweets(movieName)
      }

    hashTagTrend()






    /**
     * Read Each Input movie file, clean tweets and write it to output file
     * @param movieFileName
     */
    def readFileAndCleanTweets(movieFileName: String): Unit =
    {
      //Read input file
      val input = sc.textFile("/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Input/"+movieFileName).distinct()


      //Clean Input tweets
      for(x<-input){
        val fileyo = x.replaceAll("(https?|ftp)://(www\\d?|[a-zA-Z0-9]+)?.[a-zA-Z0-9-]+(\\:|.)([a-zA-Z0-9.]+|(\\d+)?)([/?:].*)?", "").replaceAll("\\\\x[a-z][0-9]", "").replaceAll("\\\\x[0-9][a-z]", "").replaceAll("\\\\x[0-9][0-9]", "").replaceAll("\\\\x[a-z][a-z]", "").replaceAll("[!$%^&*?,.;?\"0-9/;():-]", "").replace("b'RT", "").replace("b'", "").replace("'", "").replaceAll("http.*?\\s", "").replaceAll("@.*?\\s", "").replaceAll("www.*?\\s", "").replace("\\n", "").replace("quot", "").replace("amp", "").replaceAll("(?:\\s|\\A)[@]+([A-Za-z0-9-_]+)", "").toLowerCase
        val numPattern = "#[A-Za-z0-9_]+".r
        val hashTag = numPattern.findAllIn(fileyo)
        val fileObject1 = new PrintWriter(new FileOutputStream(new File("Output/CleanedTweets/"+movieFileName),true))   //true For appending
        val printWriter1 = new PrintWriter(fileObject1)
        printWriter1.write(fileyo+"\n")
        printWriter1.close()

        if(!hashTag.toString.equalsIgnoreCase("none")) {
          for(n <- hashTag) {
            val fileObject2 = new PrintWriter(new FileOutputStream(new File("Output/HashTagListFromTweets/" + movieFileName), true)) //true For appending
            val printWriter2 = new PrintWriter(fileObject2)
            printWriter2.write(n + "\n")
            printWriter2.close()
          }
        }
      }
    }



    def hashTagTrend(): Unit =
    {

      //Collect all the Hashtag lists of each movie trailer into one file
      val hashTagResultFilePath = "/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Result/HashTagCompleteList.txt"
      val completeHashTagFile = new File(hashTagResultFilePath)
      completeHashTagFile.createNewFile()

      val hashTagListFolder = "/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Output/HashTagListFromTweets/"
      for(eachMovieName <- moviesList)
        {
          val movieName = eachMovieName.toString().substring(inputFilePath.length+1)
          val part0 = Source.fromFile(hashTagResultFilePath).getLines
          val part1 = Source.fromFile(hashTagListFolder+movieName).getLines
          val part2 = part0.toList ++ part1.toList
          val part00002 = new File(hashTagResultFilePath)
          val bw = new BufferedWriter(new FileWriter(part00002))
          part2.foreach(p => bw.write(p + "\n"))
          bw.close
        }

      //HashTag count, group and write into file
      val hashTagListForWordCount = sc.textFile(hashTagResultFilePath)
      val transformation1 = hashTagListForWordCount.flatMap(x => x.split(" ").filter(x => x.matches("#[A-Za-z0-9]+")))
      val words = transformation1.flatMap(line => line.split(" "))
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}.sortBy(_._2,false)
      counts.coalesce(1).saveAsTextFile("Result/HashTagCount")

      //Print the results
      val trendingTopic = "TRENDING HASHTAG = "+ counts.first() + "\n" +"===============================\n"
      val topicsList = "The first 5 trending hashtags are\n"
      writeToFile(trendingTopic+topicsList,fileGenrePopularity)
      for(n <- counts.take(5))
      {
        writeToFile(n.toString(),fileGenrePopularity)
      }
    }




    //Analyse each tweet

    val positiveWords = Set("good" , "nice" , "cool" , "like" , "love","watch","recommend","mustwatch","great","laughs","nominated","best","better","addicted","excited","tears","laughing","remarkable",
      "planning","premiere","chills","favorite","interesting","magical","tickets","epic","beautiful","success","again","many","super","enjoy","thank","buy","cried","cry","fun","counting","enhance","wait","emotion")
    val negetiveWords = Set("bad","terrible","waste","disgrace","confuse","disappoint","sucks","hate","damn","garbage","wreck","lifeless")


    for( l <- moviesList)
    {
      val movieName = l.toString().substring(inputFilePath.length+1)
      createMovieResDirectory(movieName)
    }

    for( o <- moviesList)
    {
      val movieName = o.toString().substring(inputFilePath.length+1)
      predictMoviesFuture(movieName)
    }




    def createMovieResDirectory(movieName: String) {
      val myDir = new File("Result/"+movieName)
      myDir.mkdir()
      val cleanedTweets = sc.textFile("/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Output/CleanedTweets/"+movieName)
      val temp = cleanedTweets.flatMap(x => x.split("\n")).distinct()
      val filePos = new File("Result/"+movieName+"/PositiveTweets.txt")
      filePos.createNewFile()
      val fileNeg = new File("Result/"+movieName+"/NegetiveTweets.txt")
      fileNeg.createNewFile()
      val fileNeu = new File("Result/"+movieName+"/NeutralTweets.txt")
      fileNeu.createNewFile()

      for(n <- temp )
      {

        val tweetBeingExamined = n.replaceAll("#[A-Za-z0-9]+","")     //Remove hashtags
        val res = positiveWords.exists { word => tweetBeingExamined.contains(word) }
        if(res)
        {
          writeToFile(tweetBeingExamined,filePos)
        }
        else
        {
          val res = negetiveWords.exists { word => tweetBeingExamined.contains(word) }
          if(res)
          {
            writeToFile(tweetBeingExamined,fileNeg)
          }
          else
          {
            writeToFile(tweetBeingExamined,fileNeu)
          }

        }

      }

    }






    def predictMoviesFuture(movieName1: String)  = {
      val ResultPath = "/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Result/"+movieName1
      val positiveReviews = Source.fromFile(ResultPath + "/PositiveTweets.txt").getLines().size
      val negetiveReviews = Source.fromFile(ResultPath + "/NegetiveTweets.txt").getLines().size
      val neutralReviews  = Source.fromFile(ResultPath + "/NeutralTweets.txt").getLines().size
      val totalReviews = positiveReviews + negetiveReviews + neutralReviews
      val movieName = movieName1.substring(0,movieName1.length-4)
      val FinalConclusionFile = "/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Conclusion/Conclusion.txt"
      val fileConclusion = new File(FinalConclusionFile)
      fileConclusion.createNewFile()

      val movieNameAlone = movieName.substring(0,movieName.length-4).split("_")
      var csvMovieNameGenre=""
      for(j <- movieNameAlone)
      {
        csvMovieNameGenre+=j+","
      }
      val inCSVFormat =csvMovieNameGenre+","+positiveReviews+","+negetiveReviews+","+neutralReviews+","+totalReviews
      writeToFile(inCSVFormat,fileConclusioncsv)
      if (neutralReviews > positiveReviews || neutralReviews > negetiveReviews) {

        if(positiveReviews >= neutralReviews/2){
          val conclusion1 = "\n\n"+movieName + " :\npositiveReviews = " + positiveReviews + "\nnegetiveReviews = " + negetiveReviews + "\nneutralReviews = " + neutralReviews +"\nPositive reviews are greater for the movie indicating its well reception by audience"
          writeToFile(conclusion1, fileConclusion)
        }
        else
        {
          val conclusion3 = "\n\n"+movieName + " :\npositiveReviews = " + positiveReviews + "\nnegetiveReviews = " + negetiveReviews + "\nneutralReviews = " + neutralReviews +"\nThe movie reviews are not opinionated strongly in audiance, so this movie might not have got connected with them"
          writeToFile(conclusion3, fileConclusion)
        }

      }
      else if (positiveReviews > negetiveReviews) {
        val conclusion1 = "\n\n"+movieName + " :\npositiveReviews = " + positiveReviews + "\nnegetiveReviews = " + negetiveReviews + "\nneutralReviews = " + neutralReviews +"\nPositive reviews are greater for the movie indicating its well reception by audience"
        writeToFile(conclusion1, fileConclusion)
      }
      else if (negetiveReviews > positiveReviews) {
        val conclusion2 = "\n\n"+movieName + " :\npositiveReviews = " + positiveReviews + "\nnegetiveReviews = " + negetiveReviews + "\nneutralReviews = " + neutralReviews +"\nNegetive reviews are greater for the movie which shows that the audience may not have liked few elements in the movie"
        writeToFile(conclusion2, fileConclusion)
      }
      else {
        val conclusion4 = "\n\n"+movieName+":\nNo reviews were captured for this movie"
        writeToFile(conclusion4, fileConclusion)
      }


    }



    object Foo { var positiveReviewsKids = 0
    var totalReviewsKids =0
      var positiveReviewsAction = 0
      var totalReviewsAction =0
      var positiveReviewsComedy = 0
      var totalReviewsComedy =0
      var positiveReviewsSciFi = 0
      var totalReviewsSciFi =0

    }

    /**
     * read conclusion file , add positive reviews for genre movies together and compare all of it. WHichever is highest will be popular this year
     * @param
     */
    def genrePopularity(): Unit =
    {
      for(eachMovieFileName <- moviesList) {

        val movieName = eachMovieFileName.toString().substring(inputFilePath.length + 1)
        val ResultPath = "/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Result/" + movieName
        val positiveReviewsCount = Source.fromFile(ResultPath + "/PositiveTweets.txt").getLines().size
        val negetiveReviewsCount = Source.fromFile(ResultPath + "/NegetiveTweets.txt").getLines().size
        val neutralReviewsCount = Source.fromFile(ResultPath + "/NeutralTweets.txt").getLines().size
        val totalReviewCount = positiveReviewsCount +  negetiveReviewsCount + neutralReviewsCount


        if(movieName.startsWith("Action"))
          {
           Foo.positiveReviewsAction +=positiveReviewsCount
            Foo.totalReviewsAction +=totalReviewCount
          }
        else if(movieName.startsWith("Kids"))
        {
          Foo.positiveReviewsKids += positiveReviewsCount
          Foo.totalReviewsKids += totalReviewCount
        }
          else if(movieName.startsWith("SciFi"))
        {
          Foo.positiveReviewsSciFi += positiveReviewsCount
          Foo.totalReviewsSciFi += totalReviewCount

          }
        else if(movieName.startsWith("Comedy"))
        {
          Foo.positiveReviewsComedy +=positiveReviewsCount
          Foo.totalReviewsComedy += totalReviewCount
        }

      }


      val a1 =  Foo.positiveReviewsKids
      val b1 =  Foo.totalReviewsKids
      val c1 = a1 * 100
      val popularityKids = c1 / b1

      val a2 =  Foo.positiveReviewsAction
      val b2 =  Foo.totalReviewsAction
      val c2 = a2 * 100
      val popularityAction = c2 / b2

      val a3 =  Foo.positiveReviewsComedy
      val b3 =  Foo.totalReviewsComedy
      val c3 = a3 * 100
      val popularityComedy = c3 / b3

      val a4 =  Foo.positiveReviewsSciFi
      val b4 =  Foo.totalReviewsSciFi
      val c4 = a4 * 100
      val popularitySciFi = c4 / b4


      val movieAnalysisHeader = "\n\n\n\nTWEET COUNT ANALYSIS FOR EACH GENRE\n=====================================\n"
      val movieAnalysed ="The total positive reviews of all movies of each genre against total number of reviews of each genre is as follows\n\n"+ "Kids genre positive review count = " + Foo.positiveReviewsKids + "  for total reviews of " + Foo.totalReviewsKids + "\n\nAction genre positive review count = " + Foo.positiveReviewsAction + "  for total reviews of " + Foo.totalReviewsAction + "\n\nSciFi genre positive review count = " + Foo.positiveReviewsSciFi + "  for total reviews of " + Foo.totalReviewsSciFi+ "\n\nComedy genre positive review count = " + Foo.positiveReviewsComedy + "  for total reviews of " + Foo.totalReviewsComedy

      val header = "\n\n\n\nGENRE\t\t\t|\t\tPOPULARITY\n================================"
      val res = "\nKids   \t\t\t|\t\t" + popularityKids +"%"+ "\nAction   \t\t|\t\t" + popularityAction +"%"+ "\nComedy   \t\t|\t\t" + popularityComedy +"%"+ "\nSciFi   \t\t|\t\t" + popularitySciFi +"%"
      writeToFile(movieAnalysisHeader+movieAnalysed,fileGenrePopularity)
      writeToFile(header+res,fileGenrePopularity)

      val array = Array(popularityKids,popularityAction,popularityComedy,popularitySciFi)
      val mostPopular = array.reduceLeft(_ max _)
      var str =""
      if(mostPopular== popularityKids) str+="KIDS"
      else if(mostPopular == popularityAction) str +="ACTION"
      else if(mostPopular == popularityComedy) str +="COMEDY"
      else str+="SCIENCE FICTION(SciFi)"

      writeToFile("\n---------------------------------------------------------------------------\n"+"The most popular genre of 2019 is [" +str+"] With popularity percentage of "+ mostPopular +"%"+"|\n---------------------------------------------------------------------------" , fileGenrePopularity)

    }



    genrePopularity()


  }
}
