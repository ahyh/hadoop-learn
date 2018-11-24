package com.yh.hadoop.mr.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计单词在各个文件中出现的次数
 *
 * @author yanhuan
 */
public class IndexStepTwo {

    /**
     * 产生<hello-文件名:1>
     */
    public static class IndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("-");
            context.write(new Text(split[0]), new Text(split[1].replace("\t", "->")));
        }
    }

    public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * 拿到的一组数据<hello,a.txt-->4> <hello,b.txt-->4> <hello,c.txt-->4>
         * 输出：hello a.txt-->4 b.txt-->4 c.txt-->4
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //StringBuffer是线程安全的，StringBuilder是线程不安全的，不涉及线程安全的时候使用StringBuilder
            StringBuilder sb = new StringBuilder();
            for (Text text : values) {
                sb.append(text.toString()).append("\t");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    /**
     * 提交MapReduce任务，默认提交到本地
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path("d:/mr/index/output"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mr/index/result"));

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
