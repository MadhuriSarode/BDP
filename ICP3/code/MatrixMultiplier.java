import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplier {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map (LongWritable key,Text value,Context context) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            int m= Integer.parseInt(conf.get("m"));
            int p= Integer.parseInt(conf.get("p"));
            String line= value.toString();
            String[] indicesAndValues=line.split(",");
            Text outputKey= new Text();
            Text outputValue=new Text();

            if(indicesAndValues[0].equals("M"))
            {
                for(int k=0;k<p;k++)
                {
                    outputKey.set(indicesAndValues[1] + "," + k);
                    outputValue.set(indicesAndValues[0]+","+indicesAndValues[2]+","+indicesAndValues[3]);
                    context.write(outputKey,outputValue);
                }


            }
            else
            {
                for(int i=0;i<m;i++)
                {
                    outputKey.set(i + "," + indicesAndValues[2]);
                    outputValue.set("N," + indicesAndValues[1]+"," +indicesAndValues[3]);
                    context.write(outputKey,outputValue);
                }
            }
        }
    }

    public static class Reduce1 extends Reducer<Text,Text,Text,Text>
    {
        public void reduce (Text key, Iterable<Text> values,Context context)
                throws IOException,InterruptedException
        {
            System.out.println("Staring reduce function");
            String[] value;
            HashMap<Integer,Float> hashA=new HashMap<Integer, Float>();
            HashMap<Integer,Float> hashB=new HashMap<Integer, Float>();

            for(Text val : values)
            {
                value= val.toString().split(",");
                if(value[0].equals("M"))
                {
                    hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
                else {
                    hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
            }

            int n= Integer.parseInt(context.getConfiguration().get("n"));
            float result=0.0f;
            float m_ij;
            float n_jk;
            for(int j=0 ;j< n ;j++)
            {
                m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
                n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                result +=  m_ij*n_jk;
            }
            if(result != 0.0f)

            {
             context.write(null, new Text(key.toString() + "," + Float.toString(result)));
            }
        }
    }










    public static void main (String args[]) throws Exception {


        Configuration conf = new Configuration();
        conf.set("m","2");
        conf.set("n","3");
        conf.set("p","2");


        Job job = Job.getInstance(conf, "Matrix Multiplier");
        job.setJarByClass(MatrixMultiplier.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}

