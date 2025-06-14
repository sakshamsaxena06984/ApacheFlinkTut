package com.batch.operations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class TokenizerFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String [] tokens = s.split(" ");
        for(String str: tokens){
            if (str.length()>0){
                collector.collect(new Tuple2<>(str, 1));
            }
        }
    }
}
