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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 求共同好友第一步
 *
 * @author yanhuan
 */
public class CommonFriendsOne {

    public static class CommonFriendsOneMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * k:好友，v:用户
         */
        private static Text k = new Text();
        private static Text v = new Text();

        /**
         * 输入：A:B,C,E,F,O
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] userAndFriends = value.toString().split(":");
            String user = userAndFriends[0];
            v.set(user);
            String[] friends = userAndFriends[1].split(",");
            for (String f : friends) {
                k.set(f);
                context.write(k, v);
            }
        }
    }

    public static class CommonFriendsOneReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * 一组数据：B--> A E F J ...
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> userList = new ArrayList<>();

            for (Text user : values) {
                userList.add(user.toString());
            }

            Collections.sort(userList);

            for (int i = 0; i < userList.size() - 1; i++) {
                for (int j = i + 1; j < userList.size(); j++) {
                    context.write(new Text(userList.get(i) + "-" + userList.get(j)), key);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(CommonFriendsOne.class);
        job.setMapperClass(CommonFriendsOneMapper.class);
        job.setReducerClass(CommonFriendsOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path("d:/mr/friends/input"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mr/friends/result"));

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
