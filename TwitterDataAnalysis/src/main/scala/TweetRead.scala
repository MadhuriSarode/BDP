import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import org.apache.commons.io.FileUtils

import scala.collection.JavaConversions._
import scala.io.Source


object TweetRead {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis")

    val conf = new SparkConf().setAppName("TwitterDataAnalysis").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val input = sc.textFile("/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Input/InputTweets.txt")
    val moviesList = Set("AladdinTrailer.txt","AngryBirds2Trailer.txt","Frozen2Trailer.txt")
    //Clean output directory for new results
    FileUtils.cleanDirectory(new File("Output"))
    FileUtils.cleanDirectory(new File("Result"))

    //Clean input file
    for(x<-input){
      val fileyo = x.replaceAll("(https?|ftp)://(www\\d?|[a-zA-Z0-9]+)?.[a-zA-Z0-9-]+(\\:|.)([a-zA-Z0-9.]+|(\\d+)?)([/?:].*)?", "").replaceAll("\\\\x[a-z][0-9]", "").replaceAll("\\\\x[0-9][a-z]", "").replaceAll("\\\\x[0-9][0-9]", "").replaceAll("\\\\x[a-z][a-z]", "").replaceAll("[!$%^&*?,.;?\"0-9/;():-]", "").replace("b'RT", "").replace("b'", "").replace("'", "").replaceAll("http.*?\\s", "").replaceAll("@.*?\\s", "").replaceAll("www.*?\\s", "").replace("\\n", "").replace("quot", "").replace("amp", "").replaceAll("(?:\\s|\\A)[@]+([A-Za-z0-9-_]+)", "").toLowerCase
      val fileObject = new PrintWriter(new FileOutputStream(new File("Output/CleanedTweets.txt"),true))   //true For appending
      val printWriter = new PrintWriter(fileObject)
      printWriter.write(fileyo+"\n")  // Writing to the new file
      printWriter.close()
    }

    //Create HashTag List
    val transformation1 = input.flatMap(x => x.split(" ").filter(x => x.matches("#[A-Za-z0-9]+")))
    transformation1.coalesce(1).saveAsTextFile("Output/HashTagList.txt")


    //Count HashTags and sort in descending order to know the hashtag trend
    val words = transformation1.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}.sortBy(_._2,false)
    counts.coalesce(1).saveAsTextFile("Output/HashTagCount.txt")

    //Print the results
    val trendingTopic = "Trending topic = "+ counts.first() + "\n"
    val topicsList = "The first 5 trending topics are\n"
    val fileObject = new PrintWriter(new FileOutputStream(new File("Output/Result.txt"),true))   //true For appending
    val printWriter = new PrintWriter(fileObject)
    printWriter.write(trendingTopic + topicsList)
    for(n <- counts.take(5))
      {
        printWriter.write(n+"\n")
      }
    printWriter.close()


    //Analyse each tweet

    val cleanedTweets = sc.textFile("/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/Output/CleanedTweets.txt")
    val temp = cleanedTweets.flatMap(x => x.split("\n"))
    val positiveWords = Set("good" , "nice" , "cool" , "like" , "love","will watch","recommend","must watch","great","laughs","nominated","best")
    val negetiveWords = Set("bad","terrible","waste","disgraced")


    for( n <- moviesList)
      {
        createMovieResDirectory(n)
        predictMoviesFuture(n)
      }









    def createMovieResDirectory(movieName: String) {
      val filePos = new File("Result/"+movieName+"/PositiveTweets.txt")
      filePos.createNewFile()
      val fileNeg = new File("Result/"+movieName+"/NegetiveTweets.txt")
      fileNeg.createNewFile()
      val fileNeu = new File("Result/"+movieName+"/NeutralTweets.txt")
      fileNeu.createNewFile()

      for(n <- temp )
      {

        val tweetBeingExamined = n.replaceAll("#[A-Za-z0-9]+","")
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


    def writeToFile(data: String,file: File)  = {
      val fileObject = new PrintWriter(new FileOutputStream(file, true))
      val printWriter = new PrintWriter(fileObject)
      printWriter.write(data+"\n")
      printWriter.close()
    }



    def predictMoviesFuture(movieName: String)  = {
      val positiveReviews = Source.fromFile("/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/"+movieName+"/PositiveTweets.txt").getLines().size
      val negetiveReviews = Source.fromFile("/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/"+movieName+"/NegetiveTweets.txt").getLines().size
      val neutralReviews = Source.fromFile("/Users/madhurisarode/Documents/BDP lessons/Workspace/TwitterDataAnalysis/"+movieName+"/NeutralTweets.txt").getLines().size

      println("Conclusion : ")
      println("positiveReviews = " + positiveReviews)
      println("negetiveReviews = " + negetiveReviews)
      println("neutralReviews " + neutralReviews)


      if (positiveReviews > negetiveReviews)
        println("Positive reviews are greater for this movie predicting it may be a huge success")
      else if (negetiveReviews > positiveReviews)
        println("Negetive reviews are greater for this movie indicating that it might have trouble being a success")
      else if (neutralReviews > positiveReviews || neutralReviews > negetiveReviews) {
        println("The movie reviews are not opinionated strongly in audiance, therefore only its release can tell if its a success")
      }
      else {
        println("No reviews were captured for this movie")
      }

    }

  }
}
