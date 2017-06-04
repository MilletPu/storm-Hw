package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javafx.scene.control.Tab;
import lib.Table;
import readers.SQLReader;

public class join_bolt extends BaseRichBolt implements Serializable {
    private static final long serialVersionUID = 7593355203928566992L;
    private OutputCollector collector;
    public  SQLReader sqlreader = new SQLReader();
    public Table t1 = new Table(); // 只支持2个表合并
    public Table t2 = new Table();
    public String t1_timestamp = "-1";
    public String t2_timestamp = "-1";
    public String send_timestamp = String.valueOf(System.currentTimeMillis());
    public int flow1_state = 0;
    public int flow2_state = 0;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {
        // check(input);
        if(send_timestamp.equals((String)input.getValue(1))==false)  sqlreader.read();
        System.out.println("join_bolt execute");
        String tableName = (String)input.getValue(0);
        String timestamp = (String)input.getValue(1);
        if ( sqlreader.from.length != 2 ) return; // 判断是否是join查询
        boolean is_t1 = sqlreader.from[0].equals((String)input.getValue(0));
        boolean is_t2 = sqlreader.from[1].equals((String)input.getValue(0));
        System.out.println("Table name = "+(String)input.getValue(0));
        if((is_t1==false)&(is_t2==false)) return; //非目标表
        String [] columns = String.valueOf(input.getValue(2)).split(",");
        String [] values = String.valueOf(input.getValue(3)).split(",");
        if( is_t1 ){ // 如果流属于表1
            boolean t1_tt_chg = (t1_timestamp.equals((String)input.getValue(1))==false);
            if( t1_tt_chg ) t1_timestamp = (String)input.getValue(1);
            if((flow1_state==1) & t1_tt_chg ) flow1_state = 2; // 关表
            if((flow1_state==0) & t1_tt_chg ) flow1_state = 1; // 开表
            if(flow1_state==1) {
                List<String> tb_columns = new ArrayList<String>();
                List<String> tb_row = new ArrayList<String>();
                for(int i = 0;i<columns.length;i++){
                    tb_columns.add(columns[i]);
                    tb_row.add(values[i]);
                }
                t1.tableName = (String)input.getValue(0);
                t1.setColumn(tb_columns);
                t1.addRow(tb_row);
            }
        }
        if( is_t2 ){ // 如果流属于表2
            boolean t2_tt_chg = (t2_timestamp.equals((String)input.getValue(1))==false);
            if( t2_tt_chg ) t2_timestamp = (String)input.getValue(1);
            if((flow2_state==1) & t2_tt_chg ) flow2_state = 2; // 关表
            if((flow2_state==0) & t2_tt_chg ) flow2_state = 1; // 开表
            if(flow2_state==1) {
                List<String> tb_columns = new ArrayList<String>();
                List<String> tb_row = new ArrayList<String>();
                for(int i = 0;i<columns.length;i++){
                    tb_columns.add(columns[i]);
                    tb_row.add(values[i]);
                }
                t2.tableName = (String)input.getValue(0);
                t2.setColumn(tb_columns);
                t2.addRow(tb_row);
            }
        }
        if( (flow1_state==2) & (flow2_state==2) ) {
            String merge_on = sqlreader.where[0].split("=")[0].split("\\.")[1];
            String [] on = new String[1]; on[0] = merge_on;
            //t1.watchTable();
            t1.merge(t2,on);
            t1.slice(sqlreader.select);
            emits_Table(t1); collector.ack(input);
            send_timestamp = String.valueOf(System.currentTimeMillis());
            t1 = new Table(t1.tableName); t2 = new Table(t2.tableName);
            flow1_state = 0; flow2_state = 0;
        }
        collector.ack(input);
    }
    public void emits_Table(Table t){
        t.watchTable();
        List<String> columns = t.getColumn();
        System.out.println("===================" + String.valueOf(columns.size()));
        for(int i =0;i<t.rows.size();i++) {
            Values send_seq = new Values();
            send_seq.add(send_timestamp);
            String send_columns = columns.get(0); // 发送的第二维
            String send_values = t.rows.get(i).get(0);  // 发送的第三维
            for (int j = 1; j < columns.size(); j++) {
                send_columns = send_columns+","+columns.get(j);
                send_values = send_values+","+t.rows.get(i).get(j);
            }
            send_seq.add(send_columns);
            send_seq.add(send_values);
            collector.emit(send_seq);
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> sendFields = new ArrayList<String>();
        sendFields.add("send_timestamp");
        sendFields.add("columns");
        sendFields.add("values");
        declarer.declare(new Fields(sendFields));
    }
    public static void check(Tuple input){
        for(int i =0;i<input.size();i++) {
            System.out.print(input.getValue(i)+" ");
        }
        System.out.println();
    }
}
