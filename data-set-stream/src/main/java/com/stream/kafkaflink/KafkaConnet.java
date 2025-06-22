package com.stream.kafkaflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.Stream;

public class KafkaConnet {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool param = ParameterTool.fromPropertiesFile("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/resources/config_prod.properties");
        Properties p = new Properties();
        p.setProperty("bootstrap.servers","localhost:9092");
        DataStream<String> kafkaData = env.addSource(new FlinkKafkaConsumer("kafka-topic", new SimpleStringSchema(), p));

        DataStream<Tuple2<String, Integer>> flat_map_out = kafkaData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] str = s.split(" ");
                for (String ele : str) {
                    out.collect(new Tuple2<>(ele, 1));
                }
            }
        });
        flat_map_out.keyBy(0).sum(1).writeAsText(param.get("output_kafka"));

        env.execute("Kafka Connector Example");
    }

}
