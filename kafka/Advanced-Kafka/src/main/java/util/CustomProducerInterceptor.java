package util;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 自定义生产者拦截器实现成功/失败次数统计
 * @author Ego
 * @version 1.0
 * @since 2019/11/26 12:17
 */
public class CustomProducerInterceptor implements ProducerInterceptor<String, String> {

    /**
     * 统计发送成功数
     */
    private static AtomicLong sendSuccess = new AtomicLong(0);
    /**
     * 统计发送失败数
     */
    private static AtomicLong sendFailure = new AtomicLong(0);

    /**
     * 打印出发送的成功&失败次数的统计信息
     */
    private void outputSendStat(){

        System.out.println("success count: "+ sendSuccess.get() +", failed count:"+ sendFailure.get());
    }


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {

        this.outputSendStat();
        // 加一个成功标志序号
        String messageValue = sendSuccess.toString() + ":" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), messageValue);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            sendFailure.getAndIncrement();
        } else {
            sendSuccess.getAndIncrement();
        }
    }

    @Override
    public void close() {
        this.outputSendStat();
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
