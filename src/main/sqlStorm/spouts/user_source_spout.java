package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Arrays;
import java.util.Map;
import readers.TableReader;
/**
 * Created by NoNo on 2017-6-1.
 */
public class user_source_spout extends BaseRichSpout {
        private static final long serialVersionUID = -1215556162813479167L;
        private SpoutOutputCollector collector;
        public int column_num = 0; // table.rows
        public int index_num = 0;   // table.lins
        String [] columns;
        String [][] data;
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector; //Storm自动初始化
        }
        public void nextTuple() {
            long timestamp = System.currentTimeMillis(); //时间戳
            TableReader user_table = new TableReader( "./data/user.csv" );
            columns = user_table.getColumns();
            data = user_table.getData();
            column_num = user_table.column_num;
            index_num = user_table.index_num;
            int idx = 0;
            String []tuple = new String[3+2*column_num];
            tuple[idx++] = "user";//表名
            tuple[idx++] = String.valueOf(timestamp);//时间戳
            tuple[idx++] = String.valueOf(column_num); //列的数量
            //Tuple a = new Tuple(nextTuple());
            for (int i =0;i<index_num;i++){ //发送每一行（列名，数值）
                int tmp = idx;
                for(int j =0;j<column_num;j++){
                    tuple[tmp++] = columns[j];
                    tuple[tmp++] = data[i][j];
                }
                collector.emit(new Values(tuple));
            }
            Utils.sleep(500); //每隔0.1s想外发送tuple
            System.out.println("In function nextTuple from spout1");
        }
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            String [] tuple_name = new String[3+2*column_num];
            int idx = 0;
            tuple_name[idx++] = "TableName";
            tuple_name[idx++] = "timestamp";
            tuple_name[idx++] = "column_num";
            for(int i =0;i<column_num;i++){
                tuple_name[idx++] = columns[i];
                tuple_name[idx++] = "value_"+String.valueOf(i);
            }
            declarer.declare( new Fields(tuple_name) ); // tuple = (...,value_name_i,...) value1是名称
        }
        // Spout发送到 toplogy 成功完成时调用ack
        public void ack(Object msgId) {
            System.out.println("Spout1: ack " + msgId);
        }
        // Spout发送到toplogy失败时调用fail
        public void fail(Object msgId) {
            System.out.println("Spout1: fail" + msgId);
        }
}
