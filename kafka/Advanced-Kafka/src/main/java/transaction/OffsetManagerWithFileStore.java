package transaction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 利用文件存储保存 offset 偏移量
 * @author Ego
 * @version 1.0
 * @since 2019/11/27 18:48
 */
public class OffsetManagerWithFileStore {

    private String storagePrefix;


    public OffsetManagerWithFileStore(String storagePrefix) {
        this.storagePrefix = storagePrefix;
    }

    /**
     * 将 offset 保存到外部文件数据源
     * @param topic: topic
     * @param partition: partition
     * @param offset: offset
     */
    public void saveOffsetToExternalStore(String topic, int partition, long offset) {
        try {

            FileWriter fileWriter = new FileWriter(storageName(topic, partition), false);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(offset + "");
            bufferedWriter.flush();
            bufferedWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    public long readOffsetFromExternalStore(String topic, int partition) {
        try {
            Stream<String> stream = Files.lines(Paths.get(storageName(topic, partition)));
            return Long.parseLong(stream.collect(Collectors.toList()).get(0) + 1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;

    }

    private String storageName(String topic, int partition) {
        return storagePrefix + "-" + topic + "-" + partition;
    }
}
