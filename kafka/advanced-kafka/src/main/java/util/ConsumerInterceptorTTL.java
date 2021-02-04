package util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 自定义消费者拦截器实现过期时间处理
 * @author Ego
 * @version 1.0
 * @since 2019/11/26 12:26
 */
@Slf4j
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String, String> {

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {

        long timeMillis = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();

        consumerRecords.partitions().forEach(topicPartition -> {
            List<ConsumerRecord<String, String>> recordList = consumerRecords.records(topicPartition);
            List<ConsumerRecord<String, String>> newConsumerRecords = new ArrayList<>();

            recordList.forEach(record -> {
                // 过滤时间超时记录
                if (timeMillis - record.timestamp() < 10 * 1000) {
                    newConsumerRecords.add(record);
                }
            });

            if (!newConsumerRecords.isEmpty()) {
                recordsMap.put(topicPartition, newConsumerRecords);
            }

        });
        return new ConsumerRecords<>(recordsMap);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) -> log.info("tp:{}--offset:{}", tp, offset.offset()));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
