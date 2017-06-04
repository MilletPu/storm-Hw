package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import readers.SQLReader;
import writers.SELECTWriter;

import java.io.Serializable;
import java.util.Map;
public class output_bolt extends BaseRichBolt implements Serializable {
    private static final long serialVersionUID = 4342676753918989102L;
    private OutputCollector collector;
    public static SELECTWriter writer = new SELECTWriter();
    public String output_path = "./out/output.csv";
    public SQLReader sqlreader = new SQLReader();
    public static String timestemp = String.valueOf(System.currentTimeMillis());
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {
        check(input);
        System.out.println("output_bolt execute,timestamp="+timestemp);
        System.out.println("out = "+(String)input.getValue(2));
        if(timestemp.equals((String)input.getValue(0))==false){ // 不同时间戳时,清空文件+写表头
            String send_line = (String)input.getValue(1);
            writer.write(output_path,send_line+"\n",false);
            timestemp = (String)input.getValue(0);
        }
        String send_line = (String)input.getValue(2);
        writer.write(output_path,send_line+"\n",true);
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