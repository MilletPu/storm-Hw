import backtype.storm.topology.TopologyBuilder;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.utils.Utils;

import bolts.query_bolt;
import bolts.select_out_bolt;
import spouts.user_source_spout;

public class SQLTopo {
    public static void main(String[] args) throws Exception {
        // 解决ZOOKEEPER客户端连接服务端问题(IPV6)
        System.setProperty("java.net.preferIPv4Stack", "true");
        // 创建拓扑结构
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout1",new user_source_spout(), 2).setNumTasks(4);  // 4(并发度-task) 2个executor(线程)
        builder.setBolt("bolt1", new query_bolt(), 3).shuffleGrouping("spout1");
        builder.setBolt("bolt2", new select_out_bolt(), 3).shuffleGrouping("bolt1");
        /****************************************************************************  */
        Config conf = new Config();
        conf.setDebug(false);
        if (args != null && args.length > 0) {
            // 集群模式
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            // 本地模式
            System.out.println("本地模式开始");
            LocalCluster cluster = new LocalCluster();
            // 定义topology的名称为"firstTopo"
            cluster.submitTopology("testTopology", conf, builder.createTopology());
            Utils.sleep(60000); // 本地模式 5s 后杀死该Topology
            cluster.killTopology("testTopology");
            cluster.shutdown();
        }
    }
}