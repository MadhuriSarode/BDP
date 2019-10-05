import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriends {


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
            word.set("EOF");                            // Emmitting EOF as the last token in the mapper to signal the end of file for reducer
            context.write(word, one);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, Text> {


        public Map<String, List<String>> adjVertices = new HashMap<String, List<String>>();
        ArrayList<String> tempNodeList = new ArrayList<>();
        Text word = new Text();
        Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {


            String strKey = key.toString();

            if (strKey.equalsIgnoreCase("EOF")) {

                ArrayList<String> nodeList =  (ArrayList<String>) tempNodeList.stream().distinct().collect(Collectors.toList());

                for (int i = 0; i < nodeList.size(); i++) {         //i = 0 to 4

                    List<String> firstStrList = adjVertices.get(nodeList.get(i));
                    String[] firstString = firstStrList.toArray(new String[adjVertices.get(nodeList.get(i)).size()]);

                    for (int j = i + 1; j < nodeList.size(); j++) {     ///j = 1 to 4
                        HashSet<String> set = new HashSet<>();
                        List<String> secondStrList = adjVertices.get(nodeList.get(j));
                        String[] secondString = secondStrList.toArray(new String[adjVertices.get(nodeList.get(j)).size()]);
                        set.addAll(Arrays.asList(secondString));
                        set.retainAll(Arrays.asList(firstString));

                        word.set(set.toString());
                        result.set("[ " + nodeList.get(i) + " , " + nodeList.get(j) + " ] --> ");
                        context.write(result, word);

                    }
                }
            } else {

                String edge1 = strKey.charAt(0) + "";
                String edge2 = strKey.charAt(1) + "";
                tempNodeList.add(edge1);
                if(!adjVertices.containsKey(edge1))
                {
                    adjVertices.put(edge1, new ArrayList<>());
                }
                adjVertices.get(edge1).add(edge2);

            }


        }
    }


    public static void main(String[] args) throws Exception {
      
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Common Friends");
        job.setJarByClass(CommonFriends.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

