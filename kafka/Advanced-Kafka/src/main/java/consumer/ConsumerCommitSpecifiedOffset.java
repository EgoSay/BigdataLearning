package consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import util.PropertiesUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 提交特定偏移量
 * @author Ego
 * @version 1.0
 * @since 2019/11/18 11:17 上午
 */
@Slf4j
public class ConsumerCommitSpecifiedOffset {
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

    static void commitSpecifiedOffset(KafkaConsumer<String, String> kafkaConsumer,
                                      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        int count = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                records.forEach(record -> {
                    log.info(out,
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.value());

                    // 在消费每条记录后，使用期望处理的下一个消息的偏移量更新 map 里的偏移量，下一次就从这里开始读取消息
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                });

                // 假设这一批次数据有上万条，我们指定每消费100的时候提交一次偏移量，实际应用中可以根据时间或记录的内容提交
                if (count % 100 == 0) {
                    kafkaConsumer.commitSync(currentOffsets);
                }
                count ++ ;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }

    }
    public static void main(String[] args) {

        // 定义存有希望提交的分区和偏移量的 map
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(initProperties());
        kafkaConsumer.subscribe(Collections.singleton(topic));

        commitSpecifiedOffset(kafkaConsumer, currentOffsets);
    }
}
