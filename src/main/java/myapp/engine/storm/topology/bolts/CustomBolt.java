package myapp.engine.storm.topology.bolts;

import myapp.engine.misc.Constant;
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

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class CustomBolt extends BaseRichBolt {

    public static Logger LOG = LoggerFactory.getLogger(CustomBolt.class);
    private OutputCollector outputCollector;

    private InfluxDB influxDB;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        influxDB = connect();
    }

    @Override
    public void execute(Tuple tuple) {

        try {

            String dbName = Constant.INFLUX_DB_NAME;
            String rpName = Constant.RETENTION_POLICY;

            influxDB.enableBatch(Constant.BATCH_SIZE, 100, TimeUnit.MILLISECONDS, Executors.defaultThreadFactory(), (failedPoints, throwable) -> {
                LOG.error(failedPoints.toString());
                throwable.printStackTrace();
            });

            influxDB.write(dbName, rpName, Point.measurement(Constant.MEASUREMENT)
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("random", ThreadLocalRandom.current().nextInt(1000, 10000))
                    .build());

        } catch (Exception e) {
            LOG.error("Exception : ", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("rawData"));

    }

    InfluxDB connect() {
        return InfluxDBFactory.connect("http://" + Constant.INFLUX_HOST + ":" + Constant.INFLUX_PORT);
    }

}
