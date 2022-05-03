package utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * -                   _ooOoo_
 * -                  o8888888o
 * -                  88" . "88
 * -                  (| -_- |)
 * -                   O\ = /O
 * -               ____/`---'\____
 * -             .   ' \\| |// `.
 * -              / \\||| : |||// \
 * -            / _||||| -:- |||||- \
 * -              | | \\\ - /// | |
 * -            | \_| ''\---/'' | |
 * -             \ .-\__ `-` ___/-. /
 * -          ___`. .' /--.--\ `. . __
 * -       ."" '< `.___\_<|>_/___.' >'"".
 * -      | | : `- \`.;`\ _ /`;.`/ - ` : | |
 * -        \ \ `-. \_ __\ /__ _/ .-` / /
 * ======`-.____`-.___\_____/___.-`____.-'======
 * .............................................
 * -          佛祖保佑             永无BUG
 *
 * @author :LiangFangWei
 * @description:
 * @date: 2022-05-02 14:54
 */


public class KafkaUtiles {
    private final static String KAFKA_SERVICE = "hadoop100:9092,hadoop101:9092,hadoop102:9092";
    private final static Properties properties = new Properties();

    static {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVICE);
    }

    public static FlinkKafkaProducer getKafkaProducter(String topic) {
        return new FlinkKafkaProducer(topic, new SimpleStringSchema(), properties);
    }

    public static FlinkKafkaConsumer getKafkaConsumer(String topic,String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties);
    }
}
