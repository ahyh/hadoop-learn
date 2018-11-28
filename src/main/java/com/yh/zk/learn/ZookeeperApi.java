package com.yh.zk.learn;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.List;

/**
 * zookeeper的Java客户端
 *
 * @author yanhuan
 */
public class ZookeeperApi {

    private static ZooKeeper zooKeeper;

    /**
     * 创建一个Zookeeper实例
     */
    @Before
    public void setup() throws Exception {
        zooKeeper = new ZooKeeper("mini1:2181,mini2:2181,mini3:2181", 3000, null);
    }

    @Test
    public void testCreate() throws Exception {
        /**
         * 参数1-要创建的节点的路径
         * 参数2-数据
         * 参数3-访问权限
         * 参数4-节点类型
         */
        String path = zooKeeper.create("/idea", "hello idea".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
        zooKeeper.close();
    }

    /**
     * -1就是修改所有数据
     */
    @Test
    public void testUpdate() throws Exception {
        /**
         * 参数1-要创建的节点的路径
         * 参数2-数据
         * 参数3-要修改的版本，-1表示任何版本
         */
        zooKeeper.setData("/idea", "I love you".getBytes("UTF-8"), -1);
        zooKeeper.close();
    }

    /**
     * 获取节点数据
     */
    @Test
    public void testGet() throws Exception {
        /**
         * 参数1-要创建的节点的路径
         * 参数2-是否要监听
         * 参数3-请求的数据版本，null表示最新的版本
         */
        byte[] data = zooKeeper.getData("/idea", false, null);
        System.out.println(new String(data, Charset.forName("UTF-8")));
        zooKeeper.close();
    }

    @Test
    public void testListChildren() throws Exception {
        /**
         * 参数1-节点路径
         * 参数2-是否需要监听
         */
        List<String> children = zooKeeper.getChildren("/idea", false);
        //返回结果中只有子节点名字，不带全路径
        for (String child : children) {
            System.out.println(child);
        }
        zooKeeper.close();
    }

    @Test
    public void testRmr() throws Exception {
        /**
         * 参数1-节点路径
         * 参数2-删除数据的版本，-1表示所有版本
         */
        zooKeeper.delete("/idea", -1);
        zooKeeper.close();
    }

}
