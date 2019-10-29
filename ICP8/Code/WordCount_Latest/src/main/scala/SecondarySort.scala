import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap

object SecondarySort {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "/Users/madhurisarode/Downloads/WordCount_Latest")
    val conf = new SparkConf().setAppName("secondarysort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile("input/Temperature.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => (k(0), k(3)) }

    val numReducers = 2;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))

    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }

    resultRDD.saveAsTextFile("outputs/secondary_sort")

  }
}
