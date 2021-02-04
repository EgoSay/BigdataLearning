package consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.BasicConfigurator;
import util.PropertiesUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * 有时候我们只需要一个消费者从一个主题的所有分区或者某个特定的分区读取数据
 * 注意： 一个消费者可以订阅主题，或者为自己分配分区，但不能同时做这两件事情
 * @author Ego
 * @version 1.0
 * @since 2019/11/20 5:41 下午
 */
@Slf4j
public class IndependentConsumer {

    private static final String out = "topic={} - partition={} - offset={} - value={}";

    public static void main(String[] args) {
        // 添加这一行加载log4j打印日志
        BasicConfigurator.configure();

        PropertiesUtil propertiesUtil = new PropertiesUtil();
        Properties properties = propertiesUtil.initConsumerProperties();
        // properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test3")，独立消费者不需要设置group
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 获取主题 topic 下可用的分区信息
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor("test3");
        List<TopicPartition> partitions = new LinkedList<>();

        if (partitionInfos != null) {
            // 给消费者分配指定分区，实际情况可以根据分区信息更详细的分配，此处只是简单示例
            partitionInfos.forEach(partitionInfo -> {
                if (partitionInfo.partition() % 2 == 0) {
                    partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
            });

            // 调用 assign 方法分配分区
            kafkaConsumer.assign(partitions);

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                records.forEach(record -> log.info(
                                out,
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.value()));

                kafkaConsumer.commitSync();
            }
        }
    }
}
