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
    public String t1_timestemp = "-1";
    public String t2_timestemp = "-1";
    public String send_timestemp = "-1";
    public int flow1_state = 0;
    public int flow2_state = 0;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {
        sqlreader.read();
        // check(input);
        System.out.println("join_bolt execute");
        String tableName = (String)input.getValue(0);
        String timestamp = (String)input.getValue(1);
        int n = Integer.valueOf( (String)input.getValue(2) );
        int send = 0;// 检验是否需要send
        if ( sqlreader.from.length != 2){
            collector.ack(input);
            return; //判断是否是join查询
        }
        boolean is_t1 = sqlreader.from[0].equals((String)input.getValue(0));
        boolean is_t2 = sqlreader.from[1].equals((String)input.getValue(0));
        System.out.println("Table name = "+(String)input.getValue(0));
        if((is_t1==false)&(is_t2==false)) {
            collector.ack(input);
            return;
        }
        if( is_t1 ){ // 如果流属于表1
            boolean t1_tt_chg = (t1_timestemp.equals((String)input.getValue(1))==false);
            if( t1_tt_chg ) t1_timestemp = (String)input.getValue(1);
            if((flow1_state==1) & t1_tt_chg ) flow1_state = 2; // 关表
            if((flow1_state==0) & t1_tt_chg ) flow1_state = 1; // 开表
            if(flow1_state==1) {
                List<String> columns = new ArrayList<String>();
                List<String> row = new ArrayList<String>();
                for(int i = 0;i<Integer.valueOf((String)input.getValue(2));i++){
                    columns.add((String)input.getValue(3+i*2));
                    row.add((String)input.getValue(4+i*2));
                }
                t1.tableName = (String)input.getValue(0);
                t1.setColumn(columns);
                t1.addRow(row);
            }
        }
        if( is_t2 ){ // 如果流属于表2
            boolean t2_tt_chg = (t2_timestemp.equals((String)input.getValue(1))==false);
            if( t2_tt_chg ) t2_timestemp = (String)input.getValue(1);
            if((flow2_state==1) & t2_tt_chg ) flow2_state = 2;
            if((flow2_state==0) & t2_tt_chg ) flow2_state = 1;
            if(flow2_state==1) {
                List<String> columns = new ArrayList<String>();
                List<String> row = new ArrayList<String>();
                for(int i = 0;i<Integer.valueOf((String)input.getValue(2));i++){
                    columns.add((String)input.getValue(3+i*2));
                    row.add((String)input.getValue(4+i*2));
                }
                t2.tableName = (String)input.getValue(0);
                t2.setColumn(columns);
                t2.addRow(row);
            }
        }
        System.out.println("======================"+" "+String.valueOf(flow1_state)+" "+ String.valueOf(flow2_state));
        System.out.println("---------- 1 ----------"); t1.watchTable();
        System.out.println("---------- 2 ----------"); t2.watchTable();
        if( (flow1_state==2) & (flow2_state==2) ) {
            String merge_on = sqlreader.where[0].split("=")[0].split("\\.")[1];
            String [] on = new String[1]; on[0] = merge_on;
            //t1.watchTable();
            t1.merge(t2,on);
            t1.slice(sqlreader.select);
            emits_Table(t1); collector.ack(input);
            send_timestemp = String.valueOf(System.currentTimeMillis());
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
            send_seq.add(send_timestemp);
            for (int j = 0; j < columns.size(); j++) {
                send_seq.add(columns.get(j));
                send_seq.add(t.rows.get(i).get(j));
            }
            collector.emit(send_seq);
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        sqlreader.read();
        int n = sqlreader.select.length;
        List<String> sendFields = new ArrayList<String>();
        for (int i = 0;i<2*n+1;i++){ sendFields.add("field_"+String.valueOf(i)); }
        declarer.declare(new Fields(sendFields));
    }
    public static void check(Tuple input){
        for(int i =0;i<input.size();i++) {
            System.out.print(input.getValue(i)+" ");
        }
        System.out.println();
    }
}
