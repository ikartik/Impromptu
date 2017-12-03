package myapp.engine.storm.topology;

import myapp.engine.misc.Constant;
import myapp.engine.storm.topology.bolts.AggBolt;
import myapp.engine.storm.topology.bolts.BaseBolt;
import myapp.engine.storm.topology.bolts.CustomBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.apache.storm.kafka.SpoutConfig;
import org.slf4j.LoggerFactory;



public class ImpromptuTopology {

    public static final Logger LOG = LoggerFactory.getLogger(ImpromptuTopology.class);

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

        String topologyName = Constant.TOPOLOGY_NAME;

        SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(Constant.BROKER_LIST), Constant.KAFKA_TOPIC, "", "kafka-storm-spout");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(Constant.SPOUT_NAME, new KafkaSpout(kafkaConfig),2);
        builder.setBolt(Constant.BASE_BOLT, new BaseBolt(),2).shuffleGrouping(Constant.SPOUT_NAME);
        builder.setBolt(Constant.AGG_BOLT, new AggBolt(),2).shuffleGrouping(Constant.BASE_BOLT);

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }

}
