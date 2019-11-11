package com.cjw.bigdata.kafka.controller;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


import com.cjw.bigdata.kafka.common.ErrorCode;
import com.cjw.bigdata.kafka.common.MessageEntity;
import com.cjw.bigdata.kafka.common.Response;
import com.cjw.bigdata.kafka.producer.ProducerExample1;

/**
 * @author Ego
 * @version 1.0
 * @since 2019/11/11 4:45 下午
 */

@Slf4j
@RestController
@RequestMapping("/kafka")
public class ProduceController {
    @Autowired
    private ProducerExample1 simpleProducer;

    @Value("${kafka.topic.default}")
    private String topic;

    private Gson gson = new Gson();


    @RequestMapping(value = "/send", method = RequestMethod.POST, produces = {"application/json"})
    public Response sendKafka(@RequestBody MessageEntity message) {

        try {
            log.info("kafka的消息={}", gson.toJson(message));
            simpleProducer.send(topic, "key", message);
            log.info("发送kafka成功.");
            return new Response(ErrorCode.SUCCESS, "发送kafka成功");
        } catch (Exception e) {
            log.error("发送kafka失败", e);
            return new Response(ErrorCode.EXCEPTION, "发送kafka失败");
        }
    }

}
