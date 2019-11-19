package producer;

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
public class ProducerQuick {
    private static final String topic = "test2";
    private static Properties properties = new PropertiesUtil().initProducerProperties();

    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            for (int i = 0; i < 1000; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Test" + i);
                producer.send(record);
                Thread.sleep(10);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }

}
