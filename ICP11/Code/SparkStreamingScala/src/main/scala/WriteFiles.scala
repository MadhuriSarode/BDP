import java.io._

import scala.io.Source

object WriteFiles {

  val directory = "/log/file"
  var text = ""

  def main(args: Array[String]): Unit = {
    val filename = "lorem.txt"

    for (line <- Source.fromFile(filename).getLines) {
      text = text + line
    }

    for (w <- 0 to 30) {
      val file = new File(directory + w)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(text)
      bw.close()
      println("writing file " + directory + w)
      Thread.sleep(5 * 1000) // wait for 5 seconds.
    }
  }

}