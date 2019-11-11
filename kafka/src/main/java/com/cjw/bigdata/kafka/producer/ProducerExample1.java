package com.cjw.bigdata.kafka.producer;

import com.cjw.bigdata.kafka.common.MessageEntity;
import com.cjw.bigdata.kafka.util.CustomCallback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author Ego
 * @version 1.0
 * @since 2019/11/11 3:38 下午
 */
@Component
public class ProducerExample1 {

    @Autowired
    @Qualifier("kafkaTemplate")
    private KafkaTemplate<String, MessageEntity> kafkaTemplate;

    /**
     * 最简单的同步消息发送方式
     * @param topic: kafka topic
     * @param message: messages
     */
    public void send(String topic, MessageEntity message) {
        kafkaTemplate.send(topic, message);
    }

    /**
     * 异步发送消息
     * @param topic: kafka topic
     * @param key: key
     * @param messageEntity: messages
     */
    public void send(String topic, String key, MessageEntity messageEntity) {
        final ProducerRecord<String, MessageEntity> record = new ProducerRecord<>(topic, key, messageEntity);

        long startTime = System.currentTimeMillis();
        try {
            ListenableFuture future = kafkaTemplate.send(record);
            future.addCallback(new CustomCallback(startTime, key, messageEntity));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
