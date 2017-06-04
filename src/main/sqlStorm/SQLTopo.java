import backtype.storm.topology.TopologyBuilder;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.utils.Utils;

import bolts.*;
import spouts.table_spout;

public class SQLTopo {
    public static void main(String[] args) throws Exception {
        // 解决ZOOKEEPER客户端连接服务端问题(IPV6)
        System.setProperty("java.net.preferIPv4Stack", "true");
        // 创建拓扑结构
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout1",new table_spout("user"), 2).setNumTasks(4);  // executer_num=2 task_num=4
        builder.setSpout("spout2",new table_spout("score"), 2).setNumTasks(4);
        builder.setSpout("spout3",new table_spout("course"), 2).setNumTasks(4);
        // 处理select的bolts
        builder.setBolt("bolt1", new select_bolt(), 1).shuffleGrouping("spout1").shuffleGrouping("spout2").shuffleGrouping("spout3");
        builder.setBolt("bolt2", new join_bolt(), 1).shuffleGrouping("spout1").shuffleGrouping("spout2").shuffleGrouping("spout3");
        builder.setBolt("bolt3", new groupby_bolt(), 1).shuffleGrouping("spout1").shuffleGrouping("spout2").shuffleGrouping("spout3");
        builder.setBolt("bolt4", new aggregation_bolt(), 1).shuffleGrouping("spout1").shuffleGrouping("spout2").shuffleGrouping("spout3");

        builder.setBolt("output_bolt", new output_bolt(), 1)
                .shuffleGrouping("bolt1")
                .shuffleGrouping("bolt2")
                .shuffleGrouping("bolt3")
                .shuffleGrouping("bolt4");
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
            Utils.sleep(6000000); // 本地模式 5s 后杀死该Topology
            cluster.killTopology("testTopology");
            cluster.shutdown();
        }
    }
}