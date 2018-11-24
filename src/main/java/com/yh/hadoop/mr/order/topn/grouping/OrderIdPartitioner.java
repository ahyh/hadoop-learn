package com.yh.hadoop.mr.order.topn.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 数据分区器
 *
 * @author yanhuan
 */
public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable> {

    /**
     * 数据分区逻辑，按照订单中的orderId来分区，保证相同orderId的orderBean分到一个reduceTask中处理
     */
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
