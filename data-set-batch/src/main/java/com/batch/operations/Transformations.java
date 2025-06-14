package com.batch.operations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class Transformations {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(); // set up the execution env
        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params); // make the parameters available in the web interface

        DataSet<String> text = env.readTextFile(params.get("input")); // read the input file
//        DataSet<String> text = env.readTextFile("src/main/resources/input.txt"); // read the input file

        DataSet<String> filtered = text.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("N");
            }
        });

        DataSet<Tuple2<String,Integer>> tokenized = filtered.map(new Tokenizer());
//        AggregateOperator<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
//        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping= tokenized.groupBy(1);
//        AggregateOperator<Tuple2<String, nteger>> sum = tuple2UnsortedGrouping.sum(1);
        System.out.println(counts.collect());
//        if (params.has("output")){
////            counts.writeAsCsv(params.get("output"), "\n"," ");
//            counts.writeAsCsv(params.get("output"), "\n",",");
//            env.execute("Word Count!");
//        }

//        counts.writeAsCsv(params.get("output"), "\n",",").setParallelism(1);
        env.execute("Word Count!");
        System.out.println("\n" +
                "  _____ _   _ ____  _____ _    _   _ _   _ _____ ____  \n" +
                " |_   _| | | |  _ \\| ____| |  | | | | \\ | | ____|  _ \\ \n" +
                "   | | | |_| | |_) |  _| | |  | | | |  \\| |  _| | | | |\n" +
                "   | | |  _  |  _ <| |___| |__| |_| | |\\  | |___| |_| |\n" +
                "   |_| |_| |_|_| \\_\\_____|_____\\___/|_| \\_|_____|____/ \n" +
                "  Output saved to: " + params.get("output") + "\n");
//THRELUNED


    }
}
//--input /Users/sakshamsaxena/koo/code/flink-tut/data-set-batch/batch-inputs/input.txt --output /Users/sakshamsaxena/koo/code/flink-tut/data-set-batch/batch-outputs/ouput.txt