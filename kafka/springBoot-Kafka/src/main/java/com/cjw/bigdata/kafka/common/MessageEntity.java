package com.cjw.bigdata.kafka.common;

import lombok.Data;

/**
 * @author Ego
 * @version 1.0
 * @since 2019/11/11 11:45 上午
 */

@Data
public class MessageEntity {
    private String title;
    private String body;

    @Override
    public String toString() {
        return "MessageEntity{" +
                "title='" + title + '\'' +
                ", body='" + body + '\'' +
                '}';
    }
}
