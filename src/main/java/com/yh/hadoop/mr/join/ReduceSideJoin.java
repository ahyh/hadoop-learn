package com.yh.hadoop.mr.join;

import com.yh.hadoop.mr.oder.topn.OrderBean;
import com.yh.hadoop.mr.oder.topn.OrderTopN;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Join算法的mapreduce实现
 *
 * @author yanhuan
 */
public class ReduceSideJoin {

    public static class ReduceSideJoinMapper extends Mapper<LongWritable, Text, Text, JoinBean> {

        private static String filename = null;
        private static JoinBean bean = new JoinBean();
        private static Text k = new Text();

        /**
         * maptask在进行数据处理时先调用一次setup方法，
         * 然后针对每一行调用map方法
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filename = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (filename.startsWith("order")) {
                bean.set(fields[0], fields[1], "NULL", -1, "NULL", "order");
            } else {
                bean.set("NULL", fields[0], fields[1], Integer.valueOf(fields[2]), fields[3], "user");
            }
            k.set(bean.getUserId());
            context.write(k, bean);
        }
    }

    public static class ReduceSideJoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable> {

        private static JoinBean userBean = null;

        @Override
        protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
            List<JoinBean> orderList = new ArrayList<>();
            //区分两类数据
            for (JoinBean bean : values) {
                if ("order".equals(bean.getTableName())) {
                    JoinBean newBean = new JoinBean();
                    newBean.set(bean.getOrderId(), bean.getUserId(), bean.getUsername(), bean.getAge(), bean.getUserFriend(), bean.getTableName());
                    orderList.add(newBean);
                } else {
                    userBean = new JoinBean();
                    userBean.set(bean.getOrderId(), bean.getUserId(), bean.getUsername(), bean.getAge(), bean.getUserFriend(), bean.getTableName());
                }
            }

            for (JoinBean bean : orderList) {
                bean.setUsername(userBean.getUsername());
                bean.setAge(userBean.getAge());
                bean.setUserFriend(userBean.getUserFriend());
                context.write(bean, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setReducerClass(ReduceSideJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);
        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("d:/mr/join/input"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mr/join/output"));

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
