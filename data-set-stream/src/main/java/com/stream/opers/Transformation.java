package com.stream.opers;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transformation {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(params);
        DataStreamSource<String> text = env.socketTextStream("localhost", 1212);
        SingleOutputStreamOperator<String> z= text.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s!=null;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum_ans= z.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                System.out.println("value of s is : "+s);
                return new Tuple2<>(s, 1);
            }
        }).keyBy(0).sum(1);

//        .map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String s) throws Exception {
//                return new Tuple2<>(s, 1);
//            }
//        }).keyBy(1)
//                .sum(1);

        sum_ans.print();
        env.execute("Streaming WordCount");
    }
}
