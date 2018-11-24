package com.yh.hadoop.mr.friends;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 求共同好友第二步
 *
 * @author yanhuan
 */
public class CommonFriendsTwo {

    public static class CommonFriendsTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String users = split[0];
            String friend = split[1];
            List<String> userList = Arrays.asList(users.split("-"));
            Collections.sort(userList);
            context.write(new Text(userList.get(0) + "-" + userList.get(1)), new Text(friend));
        }
    }

    public static class CommonFriendsTwoReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value.toString()).append(",");
            }
            context.write(key, new Text(sb.toString().substring(0, sb.toString().length() - 1)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(CommonFriendsTwo.class);
        job.setMapperClass(CommonFriendsTwoMapper.class);
        job.setReducerClass(CommonFriendsTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("d:/mr/friends/result"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mr/friends/output"));

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
