package myapp.storm.bolts;

import myapp.misc.Constant;
import myapp.misc.Range;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class BaseBolt extends BaseRichBolt {

    public static Logger LOG = LoggerFactory.getLogger(BaseBolt.class);
    private OutputCollector outputCollector;
    public Map<String, Range> rangeMap = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        rangeMap.put(Constant.KEY_PULSE, new Range(75, 100));
        rangeMap.put(Constant.KEY_HEART_RATE, new Range(60, 100));
        rangeMap.put(Constant.KEY_BLOOD_PRESSURE, new Range(75, 100));
        rangeMap.put(Constant.KEY_TEMPERATURE, new Range(97, 99));
        rangeMap.put(Constant.KEY_AQI, new Range(0, 60));
    }

    @Override
    public void execute(Tuple tuple) {

        String fields[] = tuple.toString().split("\t");
        String key = fields[Constant.INDEX_OF_KEY];

        if (key.equalsIgnoreCase(Constant.KEY_BLOOD_PRESSURE)) {
            String arr[] = fields[Constant.INDEX_OF_METRIC].split("/");
            Range low = new Range(80, 90);
            Range high = new Range(120, 140);

            int lowBP = Integer.parseInt(arr[1]);
            int highBP = Integer.parseInt(arr[0]);

            if (lowBP < low.min || lowBP > low.max || highBP > high.max || highBP < high.min) {
                outputCollector.emit(new Values(tuple));
            }
        } else if (!key.equalsIgnoreCase(Constant.KEY_BLOOD_PRESSURE)) {

            int metric = Integer.parseInt(fields[Constant.INDEX_OF_METRIC]);
            Range range = rangeMap.get(key);

            if (range.min > metric || range.max < metric) {
                outputCollector.emit(new Values(tuple));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("rawData"));
    }
}
