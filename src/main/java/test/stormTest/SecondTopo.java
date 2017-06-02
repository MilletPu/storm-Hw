package test.stormTest;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;  

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.utils.Utils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;

import java.util.Map;
import java.util.Random;

public class SecondTopo {
    public static void main(String[] args) throws Exception {  
    	// 解决ZOOKEEPER客户端连接服务端问题(IPV6)  
        System.setProperty("java.net.preferIPv4Stack", "true");  
        // 创建拓扑结构
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout1",new Spout1(), 2).setNumTasks(4);  // 4(并发度-task) 2个executor(线程) 
        builder.setSpout("spout2",new Spout2(), 2).setNumTasks(4); 
        builder.setBolt("bolt1", new Bolt1(), 3).shuffleGrouping("spout1","stream2").shuffleGrouping("spout2");
        builder.setBolt("bolt2", new Bolt2(), 3).shuffleGrouping("spout1","stream1").shuffleGrouping("bolt1","stream1");
        builder.setBolt("bolt3", new Bolt3(), 3).shuffleGrouping("bolt1","stream2"); 
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
            Utils.sleep(5000); // 本地模式 5s 后杀死该Topology
            cluster.killTopology("testTopology");  
            cluster.shutdown();  
        }  
    }  
}
/********************************* Spout1 ******************************************/
class Spout1 extends BaseRichSpout{  
    private static final long serialVersionUID = -1215556162813479167L;  
    private SpoutOutputCollector collector;
    public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {  
        this.collector = collector; // Storm自动初始化 
    }
    public void nextTuple() {  
        String[] words = {"spout1_word1", "spout1_word2", "spout1_word3"};   
        for(String word : words){
        	String value1 = "spout1";
        	String value2 = word;
            collector.emit("stream1",new Values(value1,value2)); // 发送数据需要封装在Values
            collector.emit("stream2",new Values(value1,value2)); // tuple = (value1,value2) tuple的长度任意
        }
        Utils.sleep(500); //每隔0.1s想外发送tuple
        System.out.println("In function nextTuple from spout1");
    }  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {      
    	declarer.declareStream("stream1", new Fields("spout1_value1","spout1_value2")); // tuple = (...,value_name_i,...) value1是名称
        declarer.declareStream("stream2", new Fields("spout1_value1","spout1_value2")); // tuple = (...,value_name_i,...) value1是名称
    }  
    // Spout发送到 toplogy 成功完成时调用ack  
    public void ack(Object msgId) {  
        System.out.println("Spout1: ack " + msgId);  
    }
    // Spout发送到toplogy失败时调用fail  
    public void fail(Object msgId) {  
        System.out.println("Spout1: fail" + msgId);  
    }  
}
/********************************* Spout2 ******************************************/
class Spout2 extends BaseRichSpout{  
    private static final long serialVersionUID = -1215556162813479167L;  
    private SpoutOutputCollector collector;
    public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {  
        this.collector = collector; // Storm自动初始化 
    }
    public void nextTuple() {  
    	String[] words = {"spout2_word1", "spout2_word2", "spout2_word3"};   
        for(String word : words){
        	String value1 = "spout2";
        	String value2 = word;
            collector.emit(new Values(value1,value2)); // 发送数据需要封装在Values
        }
        Utils.sleep(500); //每隔0.1s想外发送tuple
        System.out.println("In function nextTuple from spout2");
    }  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {      
        declarer.declare(new Fields("spout2_value1","spout2_value2")); // tuple = (...,value_name_i,...) value1是名称
    }  
    // Spout发送到 toplogy 成功完成时调用ack  
    public void ack(Object msgId) {  
        System.out.println("Spout2: ack " + msgId);  
    }  
    // Spout发送到toplogy失败时调用fail  
    public void fail(Object msgId) {  
        System.out.println("Spout2: fail" + msgId);  
    }  
}
/*********************************  Bolt1 ******************************************/
class Bolt1 extends BaseRichBolt{  
    private static final long serialVersionUID = 7593355203928566992L;  
    private OutputCollector collector;  
    @Override
    public void prepare(Map stormConf, TopologyContext context,  
            OutputCollector collector) {  
        this.collector = collector;  
    }
    @Override
    public void execute(Tuple input) {
    	if(input.getSourceComponent().equals("spout1")){
    		String from = (String)input.getValueByField("spout1_value1");// 获取 tuple=(...,value_name_i,...) 中的指定元素
            String word = (String)input.getValueByField("spout1_value2");
            from = from + "->bolt1";
            if(word != null){
                collector.emit("stream1",new Values(from,word));
                collector.emit("stream2",new Values(from,word));
                System.out.println("(bolt1 excute)			from="+from+";			sent value="+word);
            }
            collector.ack(input);
    	}
    	else{
    		String from = (String)input.getValueByField("spout2_value1");// 获取 tuple=(...,value_name_i,...) 中的指定元素
            String word = (String)input.getValueByField("spout2_value2");
            from = from + "->bolt1";
            if(word != null){
                collector.emit("stream1",new Values(from,word));
                collector.emit("stream2",new Values(from,word));
                System.out.println("(bolt1 excute)			from="+from+";			sent value="+word);
            }
            collector.ack(input);
    	}    
    }  
    @Override  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        declarer.declareStream("stream1",new Fields("bolt1_value1","bolt1_value2"));
        declarer.declareStream("stream2",new Fields("bolt1_value1","bolt1_value2"));
    }  
}
/*********************************  Bolt2 ******************************************/
class Bolt2 extends BaseRichBolt{  
    private static final long serialVersionUID = 4342676753918989102L;  
    private OutputCollector collector;  
    @Override  
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {  
        this.collector = collector;  
    }  
    @Override  
    public void execute(Tuple input) {
    	if(input.getSourceComponent().equals("spout1")){
    		String from = (String)input.getValueByField("spout1_value1");// 获取 tuple=(...,value_name_i,...) 中的指定元素
            String word = (String)input.getValueByField("spout1_value2");
            from = from + "->bolt2";
            if(word != null){
                System.out.println("(bolt2 excute)			from="+from+";			value="+word+",no sent.");
            }
            collector.ack(input);
    	}else{
    		String from = (String)input.getValueByField("bolt1_value1");// 获取 tuple=(...,value_name_i,...) 中的指定元素
            String word = (String)input.getValueByField("bolt1_value2");
            from = from + "->bolt2";
            if(word != null){
                System.out.println("(bolt2 excute)			from="+from+";			value="+word+",no sent.");
            }
            collector.ack(input);
    	}
    }  
    @Override  
    public void cleanup() {  
        super.cleanup();  
    }  
    @Override  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        // 不再传递下一个Bolt组件处理  
    }  
} 
/*********************************  Bolt3 ******************************************/
class Bolt3 extends BaseRichBolt{  
    private static final long serialVersionUID = 4342676753918989102L;  
    private OutputCollector collector;  
    @Override  
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {  
        this.collector = collector;  
    }  
    @Override  
    public void execute(Tuple input) {  
    	String from = (String)input.getValueByField("bolt1_value1");// 获取 tuple=(...,value_name_i,...) 中的指定元素
        String word = (String)input.getValueByField("bolt1_value2");
        from = from + "->bolt3";
        if(word != null){
            System.out.println("(bolt3 excute)			from="+from+";			value="+word+",no sent.");
        }
        collector.ack(input); 
    }    
    @Override  
    public void cleanup() {  
        super.cleanup();  
    }  
    @Override  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        // 不再传递下一个Bolt组件处理  
    }  
} 
