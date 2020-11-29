/*** Description: Map Reduce Program to find the hot and cold days in Merced, California dataset     ***/
/*** Input File: Plain text file in given format (6th col in max temp, 7th col is min temp           ***/


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MinMaxTemp {

    public static class MinMaxMapper
    extends Mapper < LongWritable, Text, Text, Text > {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable myarg1, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString(); // single row into a line of type String
            // split line into spaces 
            if (!(line.length() == 0)) {

                String date = line.substring(6, 14); //date

                // 6th token is temp_max and 7th token is temp_min
                float temp_Max = Float.parseFloat(line.substring(39, 45).trim()); // max temp
                float temp_Min = Float.parseFloat(line.substring(47, 53).trim()); // min temp

                // temp_max > 30 and temp_min < 15 are passed to the reducer
                if (temp_Max > 30.0) {
                    context.write(new Text("Hot Day :" + date), new Text(String.valueOf(temp_Max)));
                }

                if (temp_Min < 15) {
                    context.write(new Text("Cold Day :" + date), new Text(String.valueOf(temp_Min)));
                }
            }
        }
    }

    public static class MinMaxReducer extends Reducer < Text, Text, Text, Text > {
        // key and list of values pair and aggregation is done based on keys
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterator < Text > values, Context context) throws IOException, InterruptedException {
            String temperature = values.next().toString();
            context.write(key, new Text(temperature));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "minmaxtemp_example");

        job.setJarByClass(MinMaxTemp.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}