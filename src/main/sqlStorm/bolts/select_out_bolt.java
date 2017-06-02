package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import writers.SELECTWriter;

import java.util.Map;
public class select_out_bolt extends BaseRichBolt {
    private static final long serialVersionUID = 4342676753918989102L;
    private OutputCollector collector;
    public static SELECTWriter writer = new SELECTWriter();
    public static String timestemp = "-1";
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {
        check(input);
        System.out.println("select_out_bolt execute");
        // 不同时间戳时重新写入文件
        if(timestemp.equals((String)input.getValue(0))==false){
            String send_line = (String)input.getValue(1);
            for(int i =3;i<input.size();i++){
                if(i%2==1) send_line += ","+(String)input.getValue(i);
            }
            writer.write("./out/select_out.csv",send_line+"\n",false);
            timestemp = (String)input.getValue(0);
        }
        String send_line = (String)input.getValue(2);
        for(int i =4;i<input.size();i++){
            if(i%2==0) send_line += ","+(String)input.getValue(i);
        }
        writer.write("./out/select_out.csv",send_line+"\n",true);
        collector.ack(input);
    }
    @Override
    public void cleanup() {
        super.cleanup();
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 不再传递下一个Bolt组件处理
    }
    public static void check(Tuple input){
        for(int i =0;i<input.size();i++) {
            System.out.print(input.getValue(i)+" ");
        }
        System.out.println();
    }
}