package producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.PropertiesUtil;

import java.util.Properties;

/**
 * 不想每次去控制台敲生产消息，写这个类快速产生消息供测试使用
 * @author Ego
 * @version 1.0
 * @since 2019/11/18 2:42 下午
 */
@Slf4j
public class ProducerQuick {
    private static final String topic = "transaction-test";
    private static Properties properties = new PropertiesUtil().initProducerProperties();

    public static void main(String[] args) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 10000; ; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Test" + i);
                producer.send(record);
                log.info("successfully send record: " + record);
                Thread.sleep(10);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
