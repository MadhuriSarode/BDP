import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object NetworkWordCount {
  def main(args: Array[String]): Unit = {

    // Create the context with a 1 second batch size
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val data = ssc.socketTextStream("localhost",9999)
    val wc = data.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    ssc.textFileStream("/log")
    wc.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

