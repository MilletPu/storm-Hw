package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

import readers.SQLReader;

public class query_bolt extends BaseRichBolt {
    private static final long serialVersionUID = 7593355203928566992L;
    private OutputCollector collector;
    public static String [] sendFields;
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {
        SQLReader sqlreader = new SQLReader();
        // 解析userTable
        if(input.getSourceComponent().equals("spout1")){
            int send = 0;// 检验是否需要send
            if (sqlreader.from.equals("user")){ // sql语句操作user表
                String []where = sqlreader.where[0].split("=");
                String check = (String)input.getValueByField(where[0]);
                if(check.equals(where[1].replace("\"",""))){
                    send = 1; //该input与SQL匹配上
                }
            }
            if(send==1){
                int n = sqlreader.select.length;
                String [] send_seq = new String[1+2*n];
                sendFields = new String[1+2*n];
                int idx = 0;
                sendFields[idx] = "tuple_num";
                send_seq[idx++] = String.valueOf(n);
                for(int i =0;i<n;i++) {
                    String s = (String) input.getValueByField(sqlreader.select[i]);
                    sendFields[idx] = sqlreader.select[i];
                    send_seq[idx++] = sqlreader.select[i];
                    sendFields[idx] = "value_"+String.valueOf(i);
                    send_seq[idx++] = s;
                }
                collector.emit(new Values(send_seq));
                collector.ack(input);
            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("a","b"));
    }
}
