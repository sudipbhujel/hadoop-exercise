/*** Description: Map Reduce Program to sum and count the odd and even numbers in a file ***/
/*** Input File: Plain text file with content like this 1 2 3 4 5 6 7 8 9                ***/


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;


public class EvenOdd {

    public static class SumCountMapper
    extends Mapper < Object, Text, Text, IntWritable > {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        // public void map(Object key, Text value, OutputCollector < Text, IntWritable > output) throws IOException, InterruptedException {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            // split line into spaces
            // String data[] = value.toString().split(" ");
            // for (String num: data) {
            //     int number = Integer.parseInt(num);
            //     if (number % 2 == 1) {
            //         output.collect(new Text("ODD"), new IntWritable(number));
            //     } else {
            //         output.collect(new Text("EVEN"), new IntWritable(number));
            //     }
            // }

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                int intVal = Integer.parseInt(token);
                if (intVal % 2 == 0) {
                    word.set("EVEN");
                }
                else {
                    word.set("ODD");
                }
                context.write(word, one);
            }
        }
    }

    public static class SumCountReducer
    extends Reducer < Text, IntWritable, Text, IntWritable > {
        private IntWritable result = new IntWritable();

        // public void reduce(Text key, Iterator < IntWritable > value, OutputCollector < Text, IntWritable > output) throws IOException {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // int sum = 0, count = 0;
            // if (key.equals("ODD")) {
            //     while (value.hasNext()) {
            //         IntWritable i = value.next();
            //         // sum and count for odd
            //         sum += i.get();
            //         count++;
            //     }
            // } else {
            //     while (value.hasNext()) {
            //         IntWritable i = value.next();
            //         // sum and count for odd
            //         sum += i.get();
            //         count++;
            //     }
            // }

            // // print output
            // output.collect(key, new IntWritable(sum));
            // output.collect(key, new IntWritable(count));

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "evenoddsum_example");

        job.setJarByClass(EvenOdd.class);
        job.setMapperClass(SumCountMapper.class);
        job.setCombinerClass(SumCountReducer.class);
        job.setReducerClass(SumCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}