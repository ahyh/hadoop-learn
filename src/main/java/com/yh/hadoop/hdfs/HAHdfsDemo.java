package com.yh.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 在HA模式下要想让客户端知道集群中的情况，必须将集群中的配置文件放在classpath中
 *
 * @author yanhuan
 */
public class HAHdfsDemo {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://mini24/"), conf, "root");
        fs.copyFromLocalFile(new Path("d:/wc.jar"), new Path("/"));
        fs.close();
    }
}
