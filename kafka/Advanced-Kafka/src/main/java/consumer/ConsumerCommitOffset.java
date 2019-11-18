package consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.BasicConfigurator;
import util.PropertiesUtil;

import java.util.Collections;
import java.util.Properties;

/**
 * 主动提交偏移量
 * @author Ego
 * @version 1.0
 * @since 2019/11/15 5:01 下午
 */
@Slf4j
public class ConsumerCommitOffset {

    private static final String topic = "test2";
    private static final String groupId = "test";
    private static Properties properties;
    private static final String out = "topic={} - partition={} - offset={} - value={}";


    private static Properties initProperties() {
        PropertiesUtil propertiesUtil = new PropertiesUtil();
        properties = propertiesUtil.initConsumerProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

    public static void main(String[] args) {
        // 添加这一行加载log4j打印日志
        BasicConfigurator.configure();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(initProperties());
        kafkaConsumer.subscribe(Collections.singleton(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                records.forEach(record -> {
                    log.info(out,
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.value());

                // 调用 commitAsync() 方法异步提交, 不保证消息可靠
                kafkaConsumer.commitAsync();

                /**
                 * commitAsync() 方法 还支持自定义回调处理
                 * // kafkaConsumer.commitAsync(new OffsetCommitCallback(){
                 * //     @Override
                 * //     public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                 * //         if (e != null) {
                 * //             log.error("Commit failed for offsets {}", offsets, e);
                 * //         }
                 * //     }
                 * // });
                 */

                });
            }
        } catch (Exception e) {
            log.error("Unexpected error", e);
        } finally {
            try {
                // 使用commitSync() 方法同步提交, 将会提交由 poll() 返回的最新偏移量, 可以保证可靠性,
                // 但因为提交时程序会处于阻塞状态，限制吞吐量
                kafkaConsumer.commitSync();
            }finally {
                kafkaConsumer.close();
            }

        }

    }
}
