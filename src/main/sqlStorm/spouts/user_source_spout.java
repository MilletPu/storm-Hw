package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import readers.TableReader;

public class user_source_spout extends BaseRichSpout {
    private static final long serialVersionUID = -1215556162813479167L;
    private SpoutOutputCollector collector;
    public static String fileName = new String("./data/user.csv");
    public static TableReader table_reader = new TableReader(fileName);
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        table_reader.read(fileName);
    }
    public void nextTuple(){
        System.out.println("nextTuple user_source_spout");
        long timestamp = System.currentTimeMillis(); //时间戳
        table_reader.read(fileName);
        for (int i =0;i<table_reader.index_num;i++){ //发送每一行（列名，数值）
            Values tuple = new Values(); //长度为3+2*column_num
            tuple.add("user");//表名
            tuple.add( String.valueOf(timestamp) );//时间戳
            tuple.add( String.valueOf(table_reader.column_num) ); //列的数量
            for(int j =0;j<table_reader.column_num;j++){
                tuple.add(table_reader.getColumns()[j]);
                tuple.add(table_reader.getData()[i][j]);
            }
            collector.emit(tuple);
        }
        Utils.sleep(3000); //每隔0.1s想外发送tuple
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        table_reader.read(fileName);
        List<String> tuple_name = new ArrayList<String>();
        tuple_name.add( "TableName" );
        tuple_name.add( "timestamp" );
        tuple_name.add( "column_num" );
        for(int i =0;i<table_reader.column_num;i++) {
            tuple_name.add(table_reader.getColumns()[i]);
            tuple_name.add("value_" + String.valueOf(i));
        }
        declarer.declare( new Fields(tuple_name) ); // tuple = (...,value_name_i,...) value1是名称
    }
    public void ack(Object msgId) { System.out.println("Spout1: ack " + msgId); }
    public void fail(Object msgId) { System.out.println("Spout1: fail" + msgId); }
}
