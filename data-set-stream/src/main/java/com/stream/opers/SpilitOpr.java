package com.stream.opers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SpilitOpr {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromPropertiesFile("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/resources/config_prod.properties");

        DataStream<String> inp= env.readTextFile(params.get("input_txt2"));
        SplitStream<Integer> streamEvenOdd = inp.map(new MapFunction<String, Integer>() {

            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        }).split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer v) {
                List<String> out = new ArrayList<>();
                if ((v % 2) == 0) {
                    out.add("even");
                } else {
                    out.add("odd");
                }

                return out;
            }
        });

        DataStream<Integer> evenData = streamEvenOdd.select("even");
        DataStream<Integer> oddData = streamEvenOdd.select("odd");
        evenData.writeAsText("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/java/stream-outputs/even_num.txt").setParallelism(1);
        oddData.writeAsText("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/java/stream-outputs/odd_num.txt").setParallelism(1);
        env.execute("Split Operation Tut");

    }
}
