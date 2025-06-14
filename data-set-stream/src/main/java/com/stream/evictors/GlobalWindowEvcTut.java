package com.stream.evictors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

public class GlobalWindowEvcTut {
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
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(5))
                .evictor(CountEvictor.of(2))
                .reduce(new ReduceFunction<Tuple5<String, String, String, Integer, Integer>>() {
                    @Override
                    public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> curr, Tuple5<String, String, String, Integer, Integer> pre) throws Exception {
                        return new Tuple5<>(curr.f0, curr.f1, curr.f2, curr.f3 + pre.f3, curr.f4 + pre.f4);
                    }
                });


        reduced.writeAsText(param.get("pre_evictor_output")).setParallelism(2);


        env.execute("Session Window Tut ....");
    }
}
