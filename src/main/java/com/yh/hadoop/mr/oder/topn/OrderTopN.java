package com.yh.hadoop.mr.oder.topn;

import com.yh.hadoop.mr.index.IndexStepTwo;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 求每一个订单中的金额前三的子单
 *
 * @author yanhuan
 */
public class OrderTopN {

    /**
     * OrderTopN的Mapper
     */
    public static class OrderTopNMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
        private static OrderBean orderBean = new OrderBean();
        private static Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            orderBean.set(fields[0], fields[1], fields[2], Float.valueOf(fields[3]), Integer.valueOf(fields[4]));
            k.set(fields[0]);
            //从这里提交给map task的kv对象，会被maptask序列化后存储，所以可以重用对象
            context.write(k, orderBean);
        }
    }

    public static class OrderTopNReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int topn = conf.getInt("order.topn", 3);
            List<OrderBean> beanList = new ArrayList<>();
            for (OrderBean bean : values) {
                OrderBean newBean = new OrderBean();
                newBean.set(bean.getOrderId(), bean.getUserId(), bean.getPdtName(), bean.getPrice(), bean.getNumber());
                beanList.add(newBean);
            }
            Collections.sort(beanList);
            for (int i = 0; i < topn; i++) {
                context.write(beanList.get(i), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.setInt("order.topn",2);
        Job job = Job.getInstance(conf);
        job.setMapperClass(OrderTopNMapper.class);
        job.setReducerClass(OrderTopNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path("d:/mr/order/input"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mr/order/output"));

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
