package com.stream.statestut;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

import java.io.IOException;
//import java.lang.module.Configuration;

public class StateDemo {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10));
        final ParameterTool param = ParameterTool.fromPropertiesFile("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/resources/config_prod.properties");
        DataStream<String> data = env.socketTextStream("localhost", 1234);
        DataStream<Long> out = data.map(new MapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(String s) throws Exception {
                        String[] words = s.split(",");
                        return new Tuple2<>(Long.parseLong(words[0]), words[1]);
                    }
                }).keyBy(0)
                .flatMap(new StateFulMap());
//                .writeAsText(param.get("state_output"));
        out.print();
//        while (true)

        env.execute("State Demo ");

    }
    public static class StateFulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long>{
        private transient ValueState<Long> sum;
        private transient ValueState<Long> count;

        @Override
        public void flatMap(Tuple2<Long, String> input, Collector<Long> out) throws Exception {
            Long currCount = count.value();
            Long currSum = sum.value();

            currCount+=1;
            currSum = currSum + Long.parseLong(input.f1);
            count.update(currCount);
            sum.update(currSum);
            if (currCount>=10){
                out.collect(sum.value());
                count.clear();
                sum.clear();
            }
        }
//        public void open(Configuration conf){
//            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>("sum", TypeInformation.of(new TypeHint<Long>() {}), 0L);
//            sum = getRuntimeContext().getState(descriptor);
//
//            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>("count", TypeInformation.of(new TypeHint<Long>() {}), 0L);
//            count = getRuntimeContext().getState(descriptor2);
//        }
    }
}
