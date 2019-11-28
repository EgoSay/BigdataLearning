package transaction;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * 消费者再均衡监听器保证发送分区再平衡时记录下 offset 不丢失
 * @author Ego
 * @version 1.0
 * @since 2019/11/27 18:50
 */
public class CustomRebalanceListener implements ConsumerRebalanceListener {

    private OffsetManagerWithFileStore offsetManagerWithFileStore = new OffsetManagerWithFileStore("fileStorage");
    private Consumer<String, String> consumer;

    public CustomRebalanceListener(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        // 消费者停止读取消息之后, 发生分区再平衡之前将每个分区的 offset 偏移量保存
        partitions.forEach(partition-> offsetManagerWithFileStore.saveOffsetToExternalStore(
                partition.topic(), partition.partition(), consumer.position(partition)));
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        // 重新完成分配分区之后, 消费者开始读取消息之前 读取已经保存的分区偏移量
        partitions.forEach(partition-> consumer.seek(partition,
                offsetManagerWithFileStore.readOffsetFromExternalStore(partition.topic(), partition.partition())));
    }
}
