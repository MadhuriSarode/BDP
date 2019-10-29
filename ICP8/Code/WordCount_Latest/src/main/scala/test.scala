
import org.apache.spark._

import scala.collection.mutable

object test {

    def main(args: Array[String]) {
      def dfsMutableIterative(start: Vertex): Set[Vertex] = {
        var current: Vertex = start
        val found: mutable.Set[Vertex] = mutable.Set[Vertex]()
        val stack: mutable.Stack[Vertex] = mutable.Stack[Vertex]()
        stack.push(current)

        while (!stack.isEmpty) {
          current = stack.pop()
          if (!found.contains(current)) {
            found += current
            for (next <- current.edges) {
              stack.push(next)
            }
          }
        }
        found.toSet
      }
    }
  }


