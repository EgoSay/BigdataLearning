package com.cjw.bigdata.kafka.common;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Ego
 * @version 1.0
 * @since 2019/11/11 4:50 下午
 */
@Getter
@Setter
public class Response {
    private int code;
    private String message;

    public Response(int code, String message) {
        this.code = code;
        this.message = message;
    }
}
