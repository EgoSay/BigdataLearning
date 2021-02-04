package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import util.PropertiesUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 多线程消费
 * @author Ego
 * @version 1.0
 * @since 2019/11/25 12:01
 */
public class ConsumerMultiThread extends Thread{

    private ExecutorService executorService;
    private int threadNum;
    private  String topic;
    private  String groupId;
    private static Properties properties;
    private static final String out = "topic={} - partition={} - offset={} - value={}";

    public ConsumerMultiThread(String topic, int threadNum, String groupId) {
        this.executorService = new ThreadPoolExecutor(
                threadNum,
                threadNum,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(100),
                new ThreadPoolExecutor.CallerRunsPolicy());

        this.threadNum = threadNum;
        this.topic = topic;
        this.groupId = groupId;
    }

    private Properties initProperties() {
        PropertiesUtil propertiesUtil = new PropertiesUtil();
        properties = propertiesUtil.initConsumerProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(initProperties());
        kafkaConsumer.subscribe(Collections.singleton(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

                if (!records.isEmpty()) {
                    executorService.submit(new RecordHandler(records));
                }

                // 批次提交 offset
                synchronized (RecordHandler.currentOffsets) {
                    if (!RecordHandler.currentOffsets.isEmpty()) {
                        kafkaConsumer.commitSync(RecordHandler.currentOffsets);
                        RecordHandler.currentOffsets.clear();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }

    }
}

/**
 * 多线程处理消费日志
 */
class RecordHandler extends Thread{
    private ConsumerRecords<String, String> records;

    static Map<TopicPartition, OffsetAndMetadata> currentOffsets;


    public RecordHandler(ConsumerRecords<String, String> records) {
        this.records = records;
    }


    @Override
    public void run() {
        records.partitions().forEach(partition -> {
            List<ConsumerRecord<String, String>> records = this.records.records(partition);
            long lastConsumerOffset = records.get(records.size() - 1).offset();

            // 多分区的时候，offset 不能被同时修改，需要加锁
            synchronized (currentOffsets) {
                if (!currentOffsets.containsKey(partition)) {
                    currentOffsets.put(partition, new OffsetAndMetadata(lastConsumerOffset + 1));
                } else {
                    long offset = currentOffsets.get(partition).offset();
                    if (offset < lastConsumerOffset + 1){
                        currentOffsets.put(partition, new OffsetAndMetadata(lastConsumerOffset + 1));
                    }
                }
            }
        });
    }
}