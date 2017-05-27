package test.stormTest;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.utils.Utils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class FirstTopo {
    public static void main(String[] args) throws Exception {  
        TopologyBuilder builder = new TopologyBuilder(); //新建一个topology
        builder.setSpout("spout", new RandomSpout(),4); //使用一个Spout数据源，4线程
        builder.setBolt("bolt", new SequeceBolt(),5).shuffleGrouping("spout");
        
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
            cluster.submitTopology("firstTopo", conf, builder.createTopology());  
            Utils.sleep(50000);  
            cluster.killTopology("firstTopo");  
            cluster.shutdown();  
        }  
    }  
}
/********************************* Spout ******************************************/
class RandomSpout extends BaseRichSpout{
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
    private static String[] words = {"happy","excited","angry"};
    
    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
    	System.out.println("1");
        this.collector = arg2;
    }
    public void nextTuple() {
    	Utils.sleep(500);//每隔0.1s想外发送tuple
    	System.out.println("2");
        String word = words[new Random().nextInt(words.length)]; 
        collector.emit(new Values(word));
    }
    @Override
    public void ack(Object id) {
    	
    }
    @Override
    public void fail(Object id) {
    	
    }
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
    	System.out.println("end RandomSpout.declareOutputFields");
        arg0.declare(new Fields("randomstring"));
    }
}
/*********************************  Bolt ******************************************/
class SequeceBolt extends BaseBasicBolt{
	private static final long serialVersionUID = 1L;
    public void execute(Tuple input, BasicOutputCollector collector) {
        // TODO Auto-generated method stub
         String word = (String) input.getValue(0);  
         String out = "I'm " + word +  "!";
         System.out.println("out=" + out);
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
    }
}