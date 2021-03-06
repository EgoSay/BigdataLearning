package com.cjw.bigdata.flink.examples;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * 商品订单统计示例
 * @author Ego
 * @version 1.0
 * @date 2020/4/1 22:45
 * @since JDK1.8
 */
public class OrderStatisticsExample {

    // 随机生成数据
    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
                String key = "类别" + (char) ('A' + random.nextInt(3));
                int value = random.nextInt(10) + 1;

                System.out.println(String.format("Emits\t(%s, %d)", key, value));
                ctx.collect(new Tuple2<>(key, value));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> ds = env.addSource(new DataSource());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);

        keyedStream
                // 这一步统计各个商品的成交量
                .sum(1)
                // 将所有记录输出到同一个计算节点的实例上
                .keyBy((KeySelector<Tuple2<String, Integer>, Object>) stringIntegerTuple2 -> "")
                // 利用fold算子更新成交量值
                .fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
                    @Override
                    public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                        System.out.println("accumulator: " + accumulator);
                        System.out.println("Value: " + value);
                        accumulator.put(value.f0, value.f1);
                        return accumulator;
                    }
                })
                .addSink(new SinkFunction<HashMap<String, Integer>>() {
                    @Override
                    public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
                        // 每个类型的商品成交量
                        System.out.println("每个类型的商品成交量" + value);
                        // 商品成交总量
                        System.out.println("商品成交总量" + value.values().stream().mapToInt(v -> v).sum());
                    }
                });

        env.execute();
    }
}
