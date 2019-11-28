package consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import transaction.CustomRebalanceListener;
import transaction.OffsetManagerWithFileStore;
import util.PropertiesUtil;

import java.util.Collections;
import java.util.Properties;

/**
 * 消费者使用 subscribe() 订阅主题消费时的 Exactly-once 语义保证
 * @author Ego
 * @version 1.0
 * @since 2019/11/27 12:31
 */
@Slf4j
public class ExactlyOnceDynamicConsumer {

    private static OffsetManagerWithFileStore offsetManagerWithFileStore = new OffsetManagerWithFileStore("fileStorage");
    private static final String topics = "transaction-test";
    private static final String groupId = "transaction-test3";
    private static Properties properties;
    private static final String out = "topic={} - partition={} - offset={} - value={}";

    private static Properties initProperties() {
        PropertiesUtil propertiesUtil = new PropertiesUtil();
        properties = propertiesUtil.initConsumerProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

    private static void processRecords(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                log.info(out,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.value());

                offsetManagerWithFileStore.saveOffsetToExternalStore(record.topic(), record.partition(), record.offset());
            });
        }
    }

    public static void main(String[] args) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(initProperties());
        kafkaConsumer.subscribe(Collections.singleton(topics), new CustomRebalanceListener(kafkaConsumer));
        processRecords(kafkaConsumer);
    }

}
