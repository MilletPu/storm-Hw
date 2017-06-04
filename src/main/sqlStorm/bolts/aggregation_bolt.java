package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import lib.Table;
import readers.SQLReader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Created by NoNo on 2017-6-4.
 */

public class aggregation_bolt extends BaseRichBolt implements Serializable {
    private static final long serialVersionUID = 7593355203928566992L;
    private OutputCollector collector;
    public SQLReader sqlreader = new SQLReader();
    public Table tb = new Table();
    public String send_timestamp = "init";
    public static final Double INF = 1e20;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    public boolean checkSend(String []columns, String []values){
        String operator = "=";
        if(sqlreader.where[0].contains("=")
                && !sqlreader.where[0].contains("<")
                && !sqlreader.where[0].contains(">")
                && !sqlreader.where[0].contains("!"))
            operator="=";
        else if(sqlreader.where[0].contains(">") && !sqlreader.where[0].contains("=")) operator=">";
        else if(sqlreader.where[0].contains("<") && !sqlreader.where[0].contains("=")) operator="<";
        else if(sqlreader.where[0].contains(">=")) operator=">=";
        else if(sqlreader.where[0].contains("<=")) operator="<=";
        else if(sqlreader.where[0].contains("!=")) operator="!=";
        else operator="=";

        String[] where = sqlreader.where[0].split(operator); // 只考虑where中没有and多条件
        where[1] = where[1].replace("\"", "");
        for (int i = 0; i < columns.length; i++) {
            String column_name = columns[i];
            String value = values[i];
            boolean b1 = where[0].equals(column_name);
            int b2 = value.compareTo(where[1]);
            if( operator.equals("=") && (b1&(b2==0)) ){ return true; }
            if( operator.equals("<") && ((b1&(b2<0))) ){ return true; }
            if( operator.equals(">") && (b1&(b2>0)) ) { return true; }
            if( operator.equals(">=") && (b1&(b2>=0)) ){ return true; }
            if( operator.equals("<=") && (b1&(b2<=0)) ){ return true; }
            if( operator.equals("!=") && (b1&(b2!=0)) ){ return true; }
        }
        return false;
    }
    @Override
    public void execute(Tuple input) {
        //check(input);
        if(send_timestamp.equals((String)input.getValue(1))==false)  sqlreader.read();
        System.out.println("select_bolt execute");
        boolean has_brackets = false; //查看select中是否含有括号
        for(int i = 0;i<sqlreader.select.length;i++){
            has_brackets = has_brackets | sqlreader.select[i].contains("(")|sqlreader.select[i].contains(")");
        }
        if (!(has_brackets && sqlreader.groupby.length==0))
            return; // 非aggregation操作
        String tableName = (String)input.getValue(0);
        String timestamp = (String)input.getValue(1);
        String []columns = String.valueOf(input.getValue(2)).split(",");
        String []values = String.valueOf(input.getValue(3)).split(",");
        boolean is_need_table = sqlreader.from[0].equals(tableName);
        if(is_need_table==false) return; //  查看是否是需要的表
        boolean tb_tt_chg = (send_timestamp.equals((String)input.getValue(1))==false); //时间戳改变
        if( tb_tt_chg && (send_timestamp.equals("init")==false) ){
            emits_Values(input);
            tb = new Table((String)input.getValue(0));
        }
        send_timestamp = (String)input.getValue(1);
        boolean need_send = true;
        if(sqlreader.where.length!=0) // 如果有where限制
            need_send = checkSend(columns,values);// 检验是否需要send
        if(need_send) {
            List<String> tb_columns = new ArrayList<String>();
            List<String> tb_row = new ArrayList<String>();
            for(int i = 0;i<columns.length;i++){
                tb_columns.add(columns[i]);
                tb_row.add(values[i]);
            }
            tb.tableName = (String)input.getValue(0);
            tb.setColumn(tb_columns);
            tb.addRow(tb_row);
        }
    }
    public void emits_Values(Tuple input) {
        String [] on = new String[sqlreader.select.length];
        // 获取select中查询关键列
        for(int i = 0;i<on.length;i++) {
            if(sqlreader.select[i].contains("(")){
                int st = sqlreader.select[i].indexOf("(");
                int end = sqlreader.select[i].indexOf(")");
                on[i] = sqlreader.select[i].substring(st+1,end);
            }
            else{
                on[i] = sqlreader.select[i];
            }
        }
        Map<String,List<String>> cache = tb.aggregation();
        tb.watchTable();
        Values send_seq = new Values();
        send_seq.add(send_timestamp);
        String send_columns = ""; // 发送的第二维
        String send_values = "";  // 发送的第三维
        if (on.length==0) System.out.println("================================");
        for(int i =0;i<on.length;i++) {
            String column_key = on[i]; // 列名关键字
            int st = sqlreader.select[i].indexOf("(");
            String groupbyModel = sqlreader.select[i].substring(0,st);
            double sum = 0;
            double max_value = -INF;
            double min_value = INF;
            double count = cache.get(column_key).size();
            for(int j = 0;j<count;j++ ){
                double tmp = Float.valueOf(cache.get(column_key).get(j));
                sum += tmp;
                max_value = max(max_value,tmp);
                min_value = min(min_value,tmp);
            }
            if(send_columns.equals("")) send_columns = sqlreader.select[i];
            else                        send_columns = send_columns+","+sqlreader.select[i];
            if(groupbyModel.equals("AVG")) {
                if (send_values.equals("")) send_values = String.valueOf(sum / count);
                else send_values = send_values + "," + String.valueOf(sum / count);
            }
            if(groupbyModel.equals("SUM")) {
                if (send_values.equals("")) send_values = String.valueOf(sum);
                else send_values = send_values + "," + String.valueOf(sum );
            }
            if(groupbyModel.equals("MAX")){
                if (send_values.equals("")) send_values = String.valueOf(max_value);
                else send_values = send_values + "," + String.valueOf(max_value);
            }
            if(groupbyModel.equals("MIN")){
                if (send_values.equals("")) send_values = String.valueOf(min_value);
                else send_values = send_values + "," + String.valueOf(min_value);
            }
            if(groupbyModel.equals("COUNT")){
                if (send_values.equals("")) send_values = String.valueOf(count);
                else send_values = send_values + "," + String.valueOf(count);
            }
        }
        send_seq.add(send_columns);
        send_seq.add(send_values);
        collector.emit(send_seq);
        collector.ack(input);
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
