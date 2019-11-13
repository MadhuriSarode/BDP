import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TCPstreamchar {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount_Characters")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val characters = lines.flatMap(_.toLowerCase.split(" "))
    val pairs = characters.map(letter => (letter.length(), letter))
    val characterCount = pairs.reduceByKey(_ +" , " + _)

    print("Result")
    characterCount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}