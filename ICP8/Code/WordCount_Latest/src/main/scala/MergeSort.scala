
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import scala.io.Source


object MergeSort {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "/Users/madhurisarode/Downloads/WordCount_Latest")

    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val input = sc.textFile("/Users/madhurisarode/Downloads/WordCount_Latest/input/MergeSortInput.txt")
    val numbers: List[Int] = Source.fromFile("/Users/madhurisarode/Downloads/WordCount_Latest/input/MergeSortInput.txt").getLines.toList.map(_.toInt)

    def mergeSort(xs: List[Int]): List[Int] = {
      val n = xs.length / 2
      if (n == 0) xs
      else {
        def merge(xs: List[Int], ys: List[Int]):  List[Int] =
          (xs, ys) match {
            case(Nil, ys) => ys
            case(xs, Nil) => xs
            case(x :: xs1, y :: ys1) =>
              if (x < y) x::merge(xs1, ys)
              else y :: merge(xs, ys1)
          }
        println("Splitting")
        val (left, right) = xs splitAt(n)
        println(left)
        println(right)
        merge(mergeSort(left), mergeSort(right))
      }
    }


    val output: List[Int] = mergeSort(numbers)
    println(output)
    sc.parallelize(output).coalesce(1).saveAsTextFile("Output/MergeSortOutput.txt")

  }
}
