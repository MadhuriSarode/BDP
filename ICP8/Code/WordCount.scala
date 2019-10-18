/**
 * Illustrates flatMap + countByValue for wordcount.
 */


import org.apache.spark._

object WordCount {
    def main(args: Array[String]) {

      System.setProperty("hadoop.home.dir", "/Users/madhurisarode/Downloads/WordCount_Latest")
      //val inputFile = args(0)
      //val outputFile = args(1)
      val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      //val input =  sc.textFile(inputFile)
      val input = sc.textFile("/Users/madhurisarode/Downloads/WordCount_Latest/input/input.txt")
      // Split up into words.
      val words = input.flatMap(line => line.split(" "))
      // Transform into word and count.
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}


      //Transformation 1 : Filtering only for string and words having length > 4
      val transformation1 = input.flatMap(x => x.split(" ").filter(x => x.matches("[A-Za-z]+") && x.length > 4))

      //Transformation 2 : Filtering only for Integers
      val transformation2 = input.flatMap(x => x.split(" ").filter(x => x.matches("[0-9]+")))

      //Transformation 3 : Extracting the distinct words alone
      val transformation3 = words.distinct()
      //val mapFile = words.map(line => (line,line.length))
      // Save the word count back out to a text file, causing evaluation.

      //Action 1 : Counting and printing the number of words in the input file
      val action1= words.count()
      println(action1)

      //Action 2 : Printing the first element in the dataset
      val action2=words.first()
      println(action2)

      //Action 3 : Printing the first 5 elements using take() command and foreach() command
      val action3=words.take(5).foreach(println)

      transformation1.saveAsTextFile("Question1/output1")
      transformation2.saveAsTextFile("Question1/output2")
      transformation3.saveAsTextFile("Question1/output3")

    }
}
