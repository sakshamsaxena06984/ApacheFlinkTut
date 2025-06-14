package com.stream.windowtut;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SessionWindowTut {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool param = ParameterTool.fromPropertiesFile("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/resources/config_prod.properties");
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        DataStream<String> data= env.socketTextStream("localhost", 1234); // socat -d -d TCP-LISTEN:1234,reuseaddr,fork -

        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new MapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
            @Override
            public Tuple5<String, String, String, Integer, Integer> map(String s) throws Exception {
                String[] str = s.split(",");
                return new Tuple5<>(str[0], str[1], str[2], Integer.parseInt(str[3]), 1);
            }
        });

        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced= mapped.keyBy(0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(1)))
                .reduce(new ReduceFunction<Tuple5<String, String, String, Integer, Integer>>() {
                    @Override
                    public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> curr, Tuple5<String, String, String, Integer, Integer> pre) throws Exception {
                        return new Tuple5<>(curr.f0, curr.f1, curr.f2, curr.f3 + pre.f3, curr.f4 + pre.f4);
                    }
                });

        reduced.print();

        reduced.writeAsText(param.get("session_window_output"));


        env.execute("Session Window Tut ....");
    }
}
