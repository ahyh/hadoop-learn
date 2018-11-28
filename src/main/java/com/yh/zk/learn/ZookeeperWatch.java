package com.yh.zk.learn;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * 测试zookeeper监听器
 *
 * @author yanhuan
 */
public class ZookeeperWatch {

    private static ZooKeeper zooKeeper;

    @Before
    public void setup() throws Exception {
        zooKeeper = new ZooKeeper("mini1:2181,mini2:2181,mini3:2181", 3000, watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected
                    && watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged) {
                System.out.println(watchedEvent.getPath());
                System.out.println(watchedEvent.getType());
                System.out.println("数据发生了变化");
                try {
                    zooKeeper.getData("/mygirls", true, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected
                    && watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                System.out.println("子节点发生变化");
            }
        });
    }

    /**
     * zookeeper的监听注册一次只执行一次
     *
     * @throws Exception
     */
    @Test
    public void testWatch() throws Exception {
        byte[] data = zooKeeper.getData("/mygirls", true, null);
        List<String> children = zooKeeper.getChildren("/mygirls", true);
        System.out.println(new String(data, "UTF-8"));
        Thread.sleep(Long.MAX_VALUE);
    }

}
