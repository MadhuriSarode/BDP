import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TreeSearchAlgorithms {

    public static LinkedList<Integer>[] valArrayWithKeys = new LinkedList[5];

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            for (int i = 0; i < 5; i++) {
                valArrayWithKeys[i] = new LinkedList<Integer>();
            }

            Configuration conf = context.getConfiguration();
            FileReader file = new FileReader(conf.get("file"));
            Scanner sc = new Scanner(file);
            String[] rowElements = sc.nextLine().split(",");
            int numOfVertices = Integer.parseInt(conf.get("numOfVertices"));
            Text outputKey = new Text();
            Text outputValue = new Text();
            for (int i = 0; i < rowElements.length; i = i + numOfVertices) {
                for (int j = i; j < (i + numOfVertices); j++) {
                    int val = Integer.parseInt(rowElements[j]);
                    if (val != 0) {
                        outputKey.set(String.valueOf(i / 5));
                        outputValue.set(String.valueOf(j % 5));
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }


    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int[] arr = new int[5];
            int[] sortedArray;
            int len = 0;
            int index = 0;
            int intkey = 0;
            for (Text val : values) {
                intkey = Integer.parseInt(key.toString());
                int intVal = Integer.parseInt(val.toString());
                arr[index++] = intVal;
                len++;
            }

            sortedArray = new int[len];
            for (int j = 0; j < len; j++) {
                sortedArray[j] = arr[j];
            }
            Arrays.sort(sortedArray);
            for (int i = 0; i < len; i++) {
                valArrayWithKeys[intkey].add(sortedArray[i]);
            }
        }


        public static void breadthFirstSearch() {
            int numOfVertices = 5;
            System.out.println("Printing the graph which is being traversed");
            for (int i = 0; i < numOfVertices; i++) {
                System.out.println(i + "--->" + valArrayWithKeys[i]);
            }


            boolean flag[] = new boolean[numOfVertices];
            Arrays.fill(flag, false);
            LinkedList<Integer> traversalPath = new LinkedList<Integer>();
            LinkedList<Integer> stack = new LinkedList<Integer>();
            stack.add(0);
            int currentEdge = 0;
            traversalPath.addFirst(0);

            for (int i = 0; i < numOfVertices; i++) {
                currentEdge = stack.get(0);
                stack.removeFirst();
                flag[currentEdge] = true;
                for (int j = 0; j < valArrayWithKeys[i].size(); j++) {
                    int vertex = valArrayWithKeys[i].get(j);
                    if (!flag[vertex]) {
                        traversalPath.add(vertex);
                        flag[vertex] = true;
                        stack.add(vertex);

                    }
                }
            }

            System.out.println("Breadth First Search Result : " + traversalPath.toString());


        }


        public static void depthFirstSearch() {

            boolean flag[] = new boolean[5];
            Arrays.fill(flag, false);
            LinkedList<Integer> traversalPath = new LinkedList<Integer>();
            LinkedList<Integer> stack = new LinkedList<Integer>();
            stack.add(0);
            int currentEdge;
            traversalPath.addFirst(0);
            currentEdge = stack.get(0);
            flag[currentEdge] = true;
            int verticesChecked;

            while (!stack.isEmpty()) {
                currentEdge = stack.getLast();
                verticesChecked = 0;
                for (int j = 0; j < valArrayWithKeys[currentEdge].size(); j++) {
                    int vertex = valArrayWithKeys[currentEdge].get(verticesChecked++);
                    if (!flag[vertex]) {
                        traversalPath.add(vertex);
                        flag[vertex] = true;
                        stack.add(vertex);
                        currentEdge = vertex;
                        j = 0;
                        verticesChecked = 0;
                    }
                    if (verticesChecked == valArrayWithKeys[currentEdge].size()) {
                        verticesChecked = 0;
                        stack.removeLast();

                    }

                }

            }

            System.out.println("Depth first search result" + traversalPath.toString());

        }


    }

    public static void main(String args[]) throws Exception {

        FileUtils.forceDelete(new File("/home/cloudera/IdeaProjects/HadoopPrograms/src/main/resources/output"));
        Configuration conf = new Configuration();
        conf.set("numOfVertices", "5");
        conf.set("file", args[0]);

        Job job = Job.getInstance(conf, "BFS");
        job.setJarByClass(TreeSearchAlgorithms.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        Reduce2.breadthFirstSearch();
        Reduce2.depthFirstSearch();
        System.exit(0);

    }
}

