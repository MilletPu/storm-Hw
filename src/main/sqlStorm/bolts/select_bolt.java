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
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        sqlreader.read();
    }
    @Override
    public void execute(Tuple input) {
        //check(input);
        sqlreader.read();
        System.out.println("select_bolt execute");
        String tableName = (String)input.getValue(0);
        String timestamp = (String)input.getValue(1);
        int n = Integer.valueOf( (String)input.getValue(2) );
        int send = 0;// 检验是否需要send
        if ( sqlreader.from.length >= 2){
            collector.ack(input);
            return; // 非select操作
        }
        if (sqlreader.from[0].equals(tableName)) { // 比如sql语句操作user表
            // =
            if(sqlreader.where[0].contains("=") && !sqlreader.where[0].contains(">")
                    && !sqlreader.where[0].contains("<") && !sqlreader.where[0].contains("!")) {
                String[] where = sqlreader.where[0].split("="); //只考虑where中有一种限制
                where[1] = where[1].replace("\"", "");
                for (int i = 0; i < n; i++) {
                    String column_name = (String) input.getValue(2 * i + 3);
                    String value = (String) input.getValue(2 * i + 4);
                    boolean b1 = where[0].equals(column_name);
                    boolean b2 = where[1].equals(value);
                    if (b1 & b2) {
                        send = 1;
                        break;
                    }
                }
            }

            // >
            if(sqlreader.where[0].contains(">") && !sqlreader.where[0].contains("=")) {
                String[] where = sqlreader.where[0].split(">"); //只考虑where中有一种限制
                where[1] = where[1].replace("\"", "");
                for (int i = 0; i < n; i++) {
                    String column_name = (String) input.getValue(2 * i + 3);
                    String value = (String) input.getValue(2 * i + 4);
                    boolean b1 = where[0].equals(column_name);
                    int b2 = value.compareTo(where[1]);
                    if (b1 & b2>0) {
                        send = 1;
                        break;
                    }
                }
            }

            // >=
            if(sqlreader.where[0].contains(">=")) {
                String[] where = sqlreader.where[0].split(">="); //只考虑where中有一种限制
                where[1] = where[1].replace("\"", "");
                for (int i = 0; i < n; i++) {
                    String column_name = (String) input.getValue(2 * i + 3);
                    String value = (String) input.getValue(2 * i + 4);
                    boolean b1 = where[0].equals(column_name);
                    int b2 = value.compareTo(where[1]);
                    if (b1 & b2>=0) {
                        send = 1;
                        break;
                    }
                }
            }

            // <
            if(sqlreader.where[0].contains("<") && !sqlreader.where[0].contains("=")) {
                String[] where = sqlreader.where[0].split("<"); //只考虑where中有一种限制
                where[1] = where[1].replace("\"", "");
                for (int i = 0; i < n; i++) {
                    String column_name = (String) input.getValue(2 * i + 3);
                    String value = (String) input.getValue(2 * i + 4);
                    boolean b1 = where[0].equals(column_name);
                    int b2 = value.compareTo(where[1]);
                    if (b1 & b2<0) {
                        send = 1;
                        break;
                    }
                }
            }

            // <=
            if(sqlreader.where[0].contains("<=")) {
                String[] where = sqlreader.where[0].split("<="); //只考虑where中有一种限制
                where[1] = where[1].replace("\"", "");
                for (int i = 0; i < n; i++) {
                    String column_name = (String) input.getValue(2 * i + 3);
                    String value = (String) input.getValue(2 * i + 4);
                    boolean b1 = where[0].equals(column_name);
                    int b2 = value.compareTo(where[1]);
                    if (b1 & b2<=0) {
                        send = 1;
                        break;
                    }
                }
            }

            if(sqlreader.where[0].contains("!=")) {
                String[] where = sqlreader.where[0].split("!="); //只考虑where中有一种限制
                where[1] = where[1].replace("\"", "");
                for (int i = 0; i < n; i++) {
                    String column_name = (String) input.getValue(2 * i + 3);
                    String value = (String) input.getValue(2 * i + 4);
                    boolean b1 = where[0].equals(column_name);
                    int b2 = value.compareTo(where[1]);
                    if (b1 & b2!=0) {
                        send = 1;
                        break;
                    }
                }
            }
        }

        if(send == 1){
            Values send_seq = new Values();
            send_seq.add( (String)input.getValue(1));
            for(int i =0;i<n;i++) {
                String column_name = (String) input.getValue(2 * i + 3);
                String value = (String) input.getValue(2 * i + 4);
                boolean bol = false;
                for(int j = 0;j<sqlreader.select.length;j++){
                    bol = bol|(column_name.equals(sqlreader.select[j]));
                }
                if(bol){
                    send_seq.add(column_name);
                    send_seq.add(value);
                }
            }
            collector.emit(send_seq);
            collector.ack(input);
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
