package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import readers.SQLReader;

public class query_bolt extends BaseRichBolt {
    private static final long serialVersionUID = 7593355203928566992L;
    private OutputCollector collector;
    public static SQLReader sqlreader = new SQLReader();
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        sqlreader.read();
    }
    @Override
    public void execute(Tuple input) {
        System.out.println("query_bolt execute");
        String tableName = (String)input.getValue(0);
        String timestamp = (String)input.getValue(1);
        int n = (int)input.getValue(2);
        int send = 0;// 检验是否需要send
        if (sqlreader.from[0].equals(tableName)) { // sql语句操作user表
            String[] where = sqlreader.where[0].split("="); //只考虑where中有一种限制
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
        if(send == 1){
            Values send_seq = new Values();
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
        for (int i = 0;i<2*n;i++){ sendFields.add("field_"+String.valueOf(i)); }
        declarer.declare(new Fields(sendFields));
    }
}
