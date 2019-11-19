package consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.BasicConfigurator;
import util.PropertiesUtil;

import java.util.*;

/**
 * Kafka消费者再均衡监听器
 * @author Ego
 * @version 1.0
 * @since 2019/11/15 23:36 下午
 */
@Slf4j
public class ConsumerReBalance {
    private static final String topics = "test2";
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
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        // 实现 ConsumerRebalanceListener 接口
        // TODO 可以通过数据库事务操作处理记录和偏移量，保证消息可靠性
        class RebalancedHandler implements ConsumerRebalanceListener {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 在再均衡开始之前和消费者停止读取消息之后被调用
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 在重新分配分区之后和消费者开始读取消息之前被调用
                log.info("Lost partitions in rebalance. Committing current offsets:" + currentOffsets);
                kafkaConsumer.commitSync(currentOffsets);
            }
        }

        // 订阅消费主题时传入一个再均衡监听器实例就行
        kafkaConsumer.subscribe(Collections.singleton(topics), new RebalancedHandler());

        ConsumerCommitSpecifiedOffset.commitSpecifiedOffset(kafkaConsumer, currentOffsets);

    }

}
