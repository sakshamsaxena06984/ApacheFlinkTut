package com.stream.opers;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class FoldOperator {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromPropertiesFile("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/resources/config_prod.properties");
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> data = env.readTextFile(params.get("input_txt"));
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());
        DataStream<Tuple4<String, String, Integer, Integer>> folded= mapped.keyBy(1).fold(new Tuple4<String, String, Integer, Integer>("", "", 0, 0), new FoldFunction1());
        SingleOutputStreamOperator<Tuple2<String, Double>> avgData= folded.map(new AvgCalculator());
        avgData.print();
        env.execute("Avg Profit Per Month");
    }







    public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
        @Override
        public Tuple5<String, String, String, Integer, Integer> map(String s) throws Exception {
            String[] split= s.split(",");
            return  new Tuple5<>(split[1], split[2], split[3], Integer.parseInt(split[4]), 1);
        }

    }

    public static class FoldFunction1 implements FoldFunction<Tuple5<String, String, String, Integer, Integer>, Tuple4<String, String, Integer, Integer>>{
        @Override
        public Tuple4<String, String, Integer, Integer> fold(Tuple4<String, String, Integer, Integer> defaultInp, Tuple5<String, String, String, Integer, Integer> curr) throws Exception {
            defaultInp.f0=curr.f0;
            defaultInp.f1=curr.f1;
            defaultInp.f2+=curr.f3;
            defaultInp.f3+=curr.f4;
            return defaultInp;
        }
    }

    public static class AvgCalculator implements MapFunction<Tuple4<String, String, Integer, Integer>,Tuple2<String, Double>>{
        @Override
        public Tuple2<String, Double> map(Tuple4<String, String, Integer, Integer> inp) throws Exception {
            return new Tuple2<>(inp.f0, (inp.f2 * 0.1) / inp.f3);
        }
    }
}

