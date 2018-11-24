package com.yh.hadoop.mr.order.topn.grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 高效分组
 *
 * @author yanhuan
 */
public class OrderTopN {

    public static class OrderTopNMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

        private static OrderBean orderBean = new OrderBean();
        private static NullWritable v = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            orderBean.set(fields[0], fields[1], fields[2], Float.valueOf(fields[3]), Integer.valueOf(fields[4]));
            context.write(orderBean, v);
        }
    }

    public static class OrderTopNReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

        /**
         * 虽然reduce方法中的key只有一个，但是values迭代一次key的值就会变化
         */
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (NullWritable v : values) {
                context.write(key, v);
                i++;
                if (i == 3) {
                    return;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("order.topn", 2);
        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderTopN.class);
        job.setMapperClass(OrderTopNMapper.class);
        job.setReducerClass(OrderTopNReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(2);

        /**
         * 设置分组器和分区器
         */
        job.setPartitionerClass(OrderIdPartitioner.class);
        job.setGroupingComparatorClass(OrderIdGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path("d:/mr/order/input"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mr/order/output1"));

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
