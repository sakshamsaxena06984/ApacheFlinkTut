package com.stream.opers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceOperation {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromPropertiesFile("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/resources/config_prod.properties");
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> data = env.readTextFile(params.get("input_txt"));
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped= data.map(new MapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
            @Override
            public Tuple5<String, String, String, Integer, Integer> map(String s) throws Exception {
                String[] str = s.split(",");
                return new Tuple5<>(str[1], str[2], str[3], Integer.parseInt(str[4]), 1);
            }
        });

        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced= mapped.keyBy(0).reduce(new ReduceFunction<Tuple5<String, String, String, Integer, Integer>>() {
            @Override
            public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> curr, Tuple5<String, String, String, Integer, Integer> pre) throws Exception {
                return new Tuple5<>(curr.f0, curr.f1, curr.f2, curr.f3+pre.f3, curr.f4 + pre.f4);
            }
        });

        DataStream<Tuple2<String, Double>> profitPerMonth= reduced.map(new MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> inp) throws Exception {
                return new Tuple2<>(inp.f0, (inp.f4 * 0.1) / inp.f3);
            }
        });
        profitPerMonth.print();
        profitPerMonth.writeAsCsv(params.get("output")).setParallelism(1);

        System.out.println("\n" +
                "  _____ _   _ ____  _____ _    _   _ _   _ _____ ____  \n" +
                " |_   _| | | |  _ \\| ____| |  | | | | \\ | | ____|  _ \\ \n" +
                "   | | | |_| | |_) |  _| | |  | | | |  \\| |  _| | | | |\n" +
                "   | | |  _  |  _ <| |___| |__| |_| | |\\  | |___| |_| |\n" +
                "   |_| |_| |_|_| \\_\\_____|_____\\___/|_| \\_|_____|____/ \n" +
                "  Output saved to: " + params.get("output") + "\n");
        env.execute("Avg Profit Per Month");
    }
}
