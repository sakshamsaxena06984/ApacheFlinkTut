package com.stream.opers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AggregationOperations {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool param = ParameterTool.fromPropertiesFile("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/resources/config_prod.properties");
        DataStreamSource<String> inp = env.readTextFile(param.get("input_txt1"));
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped= inp.map(new Splitter());
//        mapped.keyBy(0).sum(4).print();
        mapped.keyBy(0).min(3).print();
//        mapped.keyBy(0).minBy(3).print();
        mapped.keyBy(0).max(3).print();




        DataStream<Tuple2<String, Integer>> mapped1 = inp.map(new Splitter1());
//        mapped1.keyBy(0).sum(1).print();


        env.execute("Aggregation Operation..");
    }
    public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
        @Override
        public Tuple5<String, String, String, Integer, Integer> map(String s) throws Exception {
            String[] split= s.split(",");
            return  new Tuple5<>(split[1], split[2], split[3], Integer.parseInt(split[4]), 1);
        }

    }

    public static class Splitter1 implements MapFunction<String, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            String[] split= s.split(",");
            return  new Tuple2<>(split[1], 1);
        }

    }
}
