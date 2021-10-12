package com.cjw.bigdata.flink.examples.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * Flink Window
 * @author chenjw
 * @version 1.0
 * @date 2021/10/12 10:41
 */
public class WindowExample {
    private static final String HOSTNAME = "10.110.30.3";
    private static final int PORT = 9999;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 接收 socket 上的数据输入 nc -lk 9999
        DataStreamSource<String> streamSource = env.socketTextStream(HOSTNAME, PORT, "\n", 3);
        KeyedStream<Tuple2<String, Long>, Tuple> stream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) {
                String[] words = s.split("\t");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1L));
                }
            }
        }).keyBy(0);

        /* Tumbling Window 滚动窗口
        * 滚动窗口 (Tumbling Windows) 是指彼此之间没有重叠的窗口。
        * 例如：每隔1小时统计过去1小时内的商品点击量，那么 1 天就只能分为 24 个窗口，每个窗口彼此之间是不存在重叠的
        */
        stream.timeWindow(Time.seconds(10)).sum(1).print("Tumbling Window");

        /* Sliding Window 滑动窗口
         * 滑动窗口用于滚动进行聚合分析，例如：每隔 6 分钟统计一次过去一小时内所有商品的点击量，那么统计窗口彼此之间就是存在重叠的，即 1天可以分为 240 个窗口
         */
        stream.timeWindow(Time.minutes(1), Time.seconds(10)).sum(1).print("Sliding Window");

        // Session Window
        // 以处理时间为衡量标准，如果10秒内没有任何数据输入，就认为会话已经关闭，此时触发统计
        stream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum(1).print("ProcessTimeSessionWindow");
        // 以事件时间为衡量标准
        stream.window(EventTimeSessionWindows.withGap(Time.seconds(10))).sum(1).print("EventTimeSessionWindow");

        // 当单词累计出现的次数每达到 3 次时，则触发计算，计算整个窗口内该单词出现的总数
        stream.window(GlobalWindows.create()).trigger(CountTrigger.of(3)).sum(1).print("GlobalWindow");

        // Count Windows 用于以数量为维度来进行数据聚合，同样也分为滚动窗口和滑动窗口
        // 滚动计数窗口，每 5 次输入则计算一次, 等价上面的 GlobalWindow
        stream.countWindow(3).sum(1).print("CountWindow1");
        // 滑动计数窗口，每 2 次输入发生后，则计算过去 5 次输入数据的情况
        stream.countWindow(5, 2).sum(1).print("CountWindow2");

        env.execute("Window Test");

    }

}
