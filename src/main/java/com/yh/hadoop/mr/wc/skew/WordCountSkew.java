package com.yh.hadoop.mr.wc.skew;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * 随机key解决数据倾斜问题
 *
 * @author yanhuan
 */
public class WordCountSkew {

    public static class WordCountSkewMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static Random random = new Random();
        private static Text k = new Text();
        private static IntWritable v = new IntWritable(1);
        private static Integer numReduceTasks = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numReduceTasks = context.getNumReduceTasks();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String w : words) {
                k.set(w + "-" + random.nextInt(numReduceTasks));
                context.write(k, v);
            }
        }
    }

    public static class WordCountSkewReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private static IntWritable v = new IntWritable(1);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v : values) {
                count += v.get();
            }
            v.set(count);
            context.write(key, v);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(WordCountSkewMapper.class);
        job.setReducerClass(WordCountSkewReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path("d:/mr/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mr/wordcount/skew-output"));

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
