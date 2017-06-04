package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import readers.SQLReader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class select_bolt extends BaseRichBolt implements Serializable {
    private static final long serialVersionUID = 7593355203928566992L;
    private OutputCollector collector;
    public SQLReader sqlreader = new SQLReader();
    public String send_timestamp = String.valueOf(System.currentTimeMillis());
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
        String tableName = (String)input.getValue(0);
        String timestamp = (String)input.getValue(1);

        String []columns = String.valueOf(input.getValue(2)).split(",");
        String []values = String.valueOf(input.getValue(3)).split(",");
        boolean has_brackets = false; //查看select中是否含有括号
        for(int i = 0;i<sqlreader.select.length;i++){
            has_brackets = has_brackets | sqlreader.select[i].contains("(")|sqlreader.select[i].contains(")");
        }
        if ( sqlreader.from.length >= 2 | sqlreader.groupby.length!=0 | has_brackets){
            collector.ack(input);
            return; // 非select操作
        }
        boolean is_need_table = sqlreader.from[0].equals(tableName);
        if(is_need_table==false) return;
        boolean need_send = true;
        if(sqlreader.where.length!=0)
            need_send = checkSend(columns,values);// 检验是否需要send
        if(need_send){
            Values send_seq = new Values();
            send_seq.add( (String)input.getValue(1)); //时间戳
            String send_columns = ""; //
            String send_values = "";  //
            for(int i =0;i<columns.length;i++){
                String column_name = columns[i];
                String value = values[i];
                boolean bol = false;
                for(int j = 0;j<sqlreader.select.length;j++){
                    bol = bol|(column_name.equals(sqlreader.select[j])); //只要该column与select[] 对号;
                }
                if(bol){
                    if(send_columns.equals("")) send_columns = column_name;
                    else                        send_columns = send_columns+","+column_name;
                    if(send_values.equals(""))  send_values = value;
                    else                        send_values = send_values+","+value;
                }
            }
            send_seq.add(send_columns);
            send_seq.add(send_values);
            collector.emit(send_seq);
            collector.ack(input);
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
