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

/**
 * 随机key解决数据倾斜问题
 *
 * @author yanhuan
 */
public class WordCountSkew2 {

    public static class WordCountSkew2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static Text k = new Text();
        private static IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] wordAndCount = value.toString().split("\t");
            v.set(Integer.valueOf(wordAndCount[1]));
            k.set(wordAndCount[0].split("-")[0]);
            context.write(k, v);
        }
    }

    public static class WordCountSkew2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
        job.setMapperClass(WordCountSkew2Mapper.class);
        job.setReducerClass(WordCountSkew2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("d:/mr/wordcount/skew-output"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mr/wordcount/skew-output2"));

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
