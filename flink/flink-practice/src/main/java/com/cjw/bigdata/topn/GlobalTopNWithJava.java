package com.cjw.bigdata.topn;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author Ego
 * @version 1.0
 * @date 2020/3/1 15:41
 * @since JDK1.8
 */
public class GlobalTopNWithJava {

    private static String hostName = "localhost";
    private static Integer port = 9999;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> text = env.socketTextStream(hostName, port);


        //将输入语句split成一个一个单词并初始化count值为1的Tuple2<String, Integer>类型
        DataStream<Tuple2<String, Integer>> ds = text.flatMap(new LineSplitter());

        DataStream<Tuple2<String, Integer>> wordCount = ds
                //按照Tuple2<String, Integer>的第一个元素为key，也就是单词
                .keyBy(0)
                //key之后的元素进入一个总时间长度为600s,每20s向后滑动一次的滑动窗口
                .window(SlidingProcessingTimeWindows.of(Time.seconds(600),Time.seconds(20)))
                // 将相同的key的元素第二个count值相加
                .sum(1);

        DataStream<Tuple2<String, Integer>> ret = wordCount
                //所有key元素进入一个20s长的窗口（选20秒是因为上游窗口每20s计算一轮数据，topN窗口一次计算只统计一个窗口时间内的变化）
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                //计算该窗口TopN
                .process(new TopNAllFunction(5));

        env.execute();
    }



    private static final class LineSplitter implements
            FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

    private static class TopNAllFunction
            extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> {

        private int topSize = 10;

        public TopNAllFunction(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void process(
                ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>.Context arg0,
                Iterable<Tuple2<String, Integer>> input,
                Collector<Tuple2<String, Integer>> out) throws Exception {
            // TODO Auto-generated method stub

            TreeMap<Integer, Tuple2<String, Integer>> treemap = new TreeMap<>(
                    (y, x) -> {
                        // TODO Auto-generated method stub
                        return (x < y) ? -1 : 1;
                    });
            // TreeMap按照key降序排列，相同count值不覆盖
            for (Tuple2<String, Integer> element : input) {
                treemap.put(element.f1, element);
                if (treemap.size() > topSize) {
                    //只保留前面TopN个元素
                    treemap.pollLastEntry();
                }
            }

            for (Map.Entry<Integer, Tuple2<String, Integer>> entry : treemap
                    .entrySet()) {
                out.collect(entry.getValue());
            }

        }

    }
}
