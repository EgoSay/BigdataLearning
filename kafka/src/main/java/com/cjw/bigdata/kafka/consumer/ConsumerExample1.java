package com.cjw.bigdata.kafka.consumer;

import com.cjw.bigdata.kafka.common.MessageEntity;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


/**
 * @author Ego
 * @version 1.0
 * @since 2019/11/11 10:33 上午
 */
@Slf4j
@Component
public class ConsumerExample1 {

    private final Gson gson = new Gson();

    @KafkaListener(topics = "${kafka.topic.default}", containerFactory = "kafkaListenerContainerFactory")
    public void receive(MessageEntity message) {
        log.info(gson.toJson(message));
    }
}
