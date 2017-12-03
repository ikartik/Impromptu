package myapp.engine.storm.topology.bolts;

import myapp.engine.misc.Constant;
import myapp.engine.misc.Range;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class AggBolt extends BaseRichBolt {

    public static Logger LOG = LoggerFactory.getLogger(AggBolt.class);
    private OutputCollector outputCollector;
    private Map<String, Set<Integer>> setMap = new HashMap<>();;
    private InfluxDB influxDB;

    private int count = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        influxDB = connect();

        influxDB.enableBatch(Constant.BATCH_SIZE, 1000, TimeUnit.MILLISECONDS, Executors.defaultThreadFactory(), (failedPoints, throwable) -> {
            LOG.error(failedPoints.toString());
            throwable.printStackTrace();
        });

    }

    @Override
    public void execute(Tuple tuple) {

        if (count%10 == 0){

            for (String key : setMap.keySet()){
                influxDB.write(Constant.INFLUX_DB_NAME, Constant.RETENTION_POLICY, Point.measurement(Constant.MEASUREMENT)
                        .time((Long.parseLong(key.split("_")[1]) * 60 * 1000), TimeUnit.MILLISECONDS)
//                        .time(key.split("_")[1], TimeUnit.MILLISECONDS)
                        .addField(key.split("_")[0], setMap.get(key).size())
                        .build());

//                LOG.info("Time is =====> " + (Integer.parseInt(key.split("_")[1]) * 60 * 1000));
            }

        }

        String fields[] = tuple.toString().split("\t");
        String key = fields[Constant.INDEX_OF_KEY];
        int id = Integer.parseInt(fields[Constant.INDEX_OF_ID]);
        int time = Integer.parseInt(fields[Constant.INDEX_OF_TIME].substring(0, fields[Constant.INDEX_OF_TIME].length()-2));

//        String mapKey = key + "_" + (time/60);

        String mapKey = key + "_" + ((1512273450/60) + (((int)(Math.random()*60))));

        if (setMap.containsKey(mapKey)){
            setMap.get(mapKey).add(id);
        } else {
            Set<Integer> temp = new HashSet<>();
            temp.add(id);
            setMap.put(mapKey, temp);
        }
        count += 1;

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("aggData"));
    }

    InfluxDB connect() {
        return InfluxDBFactory.connect("http://" + Constant.INFLUX_HOST + ":" + Constant.INFLUX_PORT);
    }
}
