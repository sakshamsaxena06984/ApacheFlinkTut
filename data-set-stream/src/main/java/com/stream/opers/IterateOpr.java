package com.stream.opers;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterateOpr {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool param = ParameterTool.fromPropertiesFile("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/resources/config_prod.properties");
        DataStream<Tuple2<Long, Integer>> mapped = env.generateSequence(0, 5)
                .map(new MapFunction<Long, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> map(Long v) throws Exception {
                        return new Tuple2<Long, Integer>(v, 0);
                    }
                });

        IterativeStream<Tuple2<Long, Integer>> iterated = mapped.iterate(5000);// 5000 the time, after this after this time stream will be terminate once records are not available
        DataStream<Tuple2<Long, Integer>> plusOne = iterated.map(new MapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
            @Override
            public Tuple2<Long, Integer> map(Tuple2<Long, Integer> v) throws Exception {
                if (v.f0 == 10) {
                    return v;
                } else {
                    return new Tuple2<>(v.f0 + 1, v.f1 + 1);
                }
            }
        });

        /**
         * via above & below operation
         * input stream 1,0 | 2,0 | 3,0 | 4,0 | 5,0
         * 2,1 | 3,1 | 4,1 | 5,1 | 6,1
         * 3,2 | 4,2 | 5,2 | 6,2 | 7,2 ..... and so on until it reach to 10
         * (10,5)
         * (10,6)
         * (10,7)
         * (10,8)
         * (10,9)
         * (10,10)
         */
        DataStream<Tuple2<Long, Integer>> notEqualTen = plusOne.filter(new FilterFunction<Tuple2<Long, Integer>>() {
            @Override
            public boolean filter(Tuple2<Long, Integer> v) throws Exception {
                if (v.f0 == 10) return false;
                else return true;
            }
        });

        iterated.closeWith(notEqualTen);

        DataStream<Tuple2<Long, Integer>> equalTen= plusOne.filter(new FilterFunction<Tuple2<Long, Integer>>() {
            @Override
            public boolean filter(Tuple2<Long, Integer> v) throws Exception {
                if (v.f0 == 10) return true;
                else return false;
            }
        });

        equalTen.writeAsText(param.get("iterative_output")).setParallelism(1);


        env.execute("Iterate Operator Tutorial");
    }
}
