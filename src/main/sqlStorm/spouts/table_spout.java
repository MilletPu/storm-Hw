package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import readers.TableReader;

public class table_spout extends BaseRichSpout implements Serializable {
    private static final long serialVersionUID = -1215556162813479167L;
    private SpoutOutputCollector collector;
    public String tableName;
    public String table_path;
    public TableReader table_reader;
    public table_spout(){}
    public table_spout(String table_name){
        tableName = table_name;
        table_path = "./data/"+table_name+".csv";
        table_reader = new TableReader(table_path);
    }
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }
    public void nextTuple(){
        System.out.println("nextTuple user_source_spout");
        long timestamp = System.currentTimeMillis(); //时间戳
        table_reader.read(table_path);
        for (int i =0;i<table_reader.index_num;i++){ //发送每一行（列名，数值）
            Values tuple = new Values(); //长度为3+2*column_num
            tuple.add(tableName);//表名
            tuple.add( String.valueOf(timestamp) );//时间戳
            String columns = table_reader.getColumns()[0];
            String values = table_reader.getData()[i][0];
            for(int j =1;j<table_reader.column_num;j++) {
                columns = columns+","+table_reader.getColumns()[j];
                values = values+","+table_reader.getData()[i][j];
            }
            tuple.add(columns); tuple.add(values);
            collector.emit(tuple);
        }
        Utils.sleep(3000); //每隔0.1s想外发送tuple
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> tuple_name = new ArrayList<String>();
        tuple_name.add( "TableName" );
        tuple_name.add( "timestamp" );
        tuple_name.add( "columns" );
        tuple_name.add( "values" );
        declarer.declare( new Fields(tuple_name) ); // tuple = (...,value_name_i,...) value1是名称
    }
    public void ack(Object msgId) { System.out.println(tableName+" ack " + msgId); }
    public void fail(Object msgId) { System.out.println(tableName+" fail" + msgId); }
}
