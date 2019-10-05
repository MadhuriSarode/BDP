
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class YouTubeVideos {

    public static class TokenizerMapperClass
            extends Mapper<Object, Text, Text, FloatWritable> {
        private Text video_id = new Text();
        private final static FloatWritable rating = new FloatWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

           String line = value.toString();
            String str[] = line.split("\t");
            if (str.length > 5) {
                if(str[2].matches("\\d+.+")) {
                    Float f= Float.parseFloat(str[2]);
                    rating.set(f);
                }
                video_id.set(str[0]);
            }
            context.write(video_id,rating);
        }
    }


    public static class IntSumReducerr1 extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sumValue = 0;
            int count=0;
            for (FloatWritable vall : values) {
                count+=1;
                sumValue += vall.get();
            }
            sumValue=sumValue/count;
            context.write(key, new FloatWritable(sumValue));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("InputFile", args[0]);
        Job job = Job.getInstance(conf, "YouTubeVideos");
        job.setJarByClass(YouTubeVideos.class);
        job.setMapperClass(TokenizerMapperClass.class);
        job.setCombinerClass(IntSumReducerr1.class);
        job.setReducerClass(IntSumReducerr1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
