package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import writers.SELECTWriter;

import java.util.Map;

/**
 * Created by NoNo on 2017-6-1.
 */
public class select_out_bolt extends BaseRichBolt {
    private static final long serialVersionUID = 4342676753918989102L;
    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {
        SELECTWriter writer = new SELECTWriter();
        int n = Integer.valueOf( (String)input.getValue(0) );
        String send_line = (String)input.getValue(1);
        for(int i =0;i<n;i++)
            send_line += ","+(String)input.getValue(1 + 2*i);
        writer.write("./out/select_out.csv",send_line,true);
        send_line = (String)input.getValue(2);
        for(int i =0;i<n;i++){
            send_line += ","+(String)input.getValue(2 + 2*i);
        }
        writer.write("./out/select_out.csv",send_line,true);
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
}