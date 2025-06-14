package com.stream.windowtut;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;

public class TumblingWindowEventTime {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool param = ParameterTool.fromPropertiesFile("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/resources/config_prod.properties");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<String> data= env.socketTextStream("localhost", 1234);
        DataStream<Tuple2<Long, String>> sum= data.map(new MapFunction<String, Tuple2<Long, String>>() {

                    @Override
                    public Tuple2<Long, String> map(String s) throws Exception {
                        String[] words = s.split(",");
                        return new Tuple2<>(Long.parseLong(words[0]), words[1]);
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Long, String> t) {
                        return t.f0;
                    }
                }).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .reduce(new ReduceFunction<Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> reduce(Tuple2<Long, String> def, Tuple2<Long, String> cur) throws Exception {
                        Timestamp t = new Timestamp(System.currentTimeMillis());
                        return new Tuple2<>(t.getTime(), "" + (Integer.parseInt(def.f1) + Integer.parseInt(cur.f1)));
                    }
                });

        sum.writeAsText(param.get("tumbling_output_allwindow"));


        env.execute("Tumbling Window Event Time");
    }
}
