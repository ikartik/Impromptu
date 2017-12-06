package myapp.misc;

public class Constant {

    public final static String TOPOLOGY_NAME = "impromptuTopology";

    public final static String BROKER_LIST = "localhost";
    public final static String KAFKA_TOPIC = "newTopic";

    public final static String INFLUX_HOST = "localhost";
    public final static int INFLUX_PORT = 8086;
    public final static String INFLUX_DB_NAME = "customDb";
    public final static String RETENTION_POLICY = "autogen";

    public final static int BATCH_SIZE = 10;
    public final static String MEASUREMENT = "customMetrics";

    public final static String SPOUT_NAME = "kafkaSpout";
    public final static String BOLT_NAME = "customBolt";
    public final static String BASE_BOLT = "baseBolt";
    public final static String AGG_BOLT = "aggBolt";


    public final static int INDEX_OF_LATTITUDE = 0;
    public final static int INDEX_OF_LONGITUDE = 1;
    public final static int INDEX_OF_ID = 2;
    public final static int INDEX_OF_NAME= 3;
    public final static int INDEX_OF_KEY = 4;
    public final static int INDEX_OF_METRIC = 5;
    public final static int INDEX_OF_TIME = 6;

    public final static String KEY_PULSE = "Pulse Rate";
    public final static String KEY_HEART_RATE = "Heart Rate";
    public final static String KEY_BLOOD_PRESSURE = "Blood Pressure";
    public final static String KEY_TEMPERATURE = "Body Temperature";
    public final static String KEY_AQI = "AQI";

}
