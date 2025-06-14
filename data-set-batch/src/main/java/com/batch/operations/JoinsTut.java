package com.batch.operations;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class JoinsTut {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        MapOperator<String, Tuple2<Integer, String>> person_table= env.readTextFile(params.get("person"))
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Tuple2<Integer, String>(Integer.parseInt(split[0]), split[1]);
                    }
                });

        MapOperator<String, Tuple2<Integer, String>> location_table= env.readTextFile(params.get("location"))
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Tuple2<Integer, String>(Integer.parseInt(split[0]), split[1]);
                    }
                });

        System.out.println(location_table.collect());

//        // performing joins - inner join
//        JoinOperator.EquiJoin<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> join_out =
//                person_table.join(location_table)
//                        .where(0)
//                        .equalTo(0)
//                        .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
//            @Override
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> persons, Tuple2<Integer, String> locations) throws Exception {
//                return new Tuple3<>(persons.f0, persons.f1, locations.f1);
//            }
//        });
//        System.out.println(join_out.collect());

        // performing joins - left join

//        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> left_join_result =
//                person_table.leftOuterJoin(location_table)
//                        .where(0)
//                        .equalTo(0)
//                        .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
//                    @Override
//                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) throws Exception {
//                        if (o2==null){
//                            return  new Tuple3<>(o1.f0, o1.f1," NULL");
//                        }
//                        return new Tuple3<>(o1.f0, o1.f1, o2.f1);
//                    }
//                }
//        );

        // performing joins - right join

//        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> right_join_out = person_table.rightOuterJoin(location_table)
//                .where(0)
//                .equalTo(0)
//                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
//                    @Override
//                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) throws Exception {
//                        if (o1 == null) {
//                            return new Tuple3<>(o2.f0, "NULL", o2.f1);
//                        }
//                        else if(o2==null){
//                            return  new Tuple3<>(o1.f0, o1.f1," NULL");
//                        }
//                        return new Tuple3<>(o1.f0, o1.f1, o2.f1);
//                    }
//                });

        // performing outer join
//        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> full_out_join_result= person_table.fullOuterJoin(location_table).where(0)
//                .equalTo(0)
//                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
//                    @Override
//                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) throws Exception {
//                        if (o1 == null) {
//                            return new Tuple3<>(o2.f0, "NULL", o2.f1);
//                        } else if (o2 == null) {
//                            return new Tuple3<>(o1.f0, o1.f1, " NULL");
//                        }
//                        return new Tuple3<>(o1.f0, o1.f1, o2.f1);
//                    }
//                });


        // Hints with joins
        /**
         * JoinHint.OPTIMIZER_CHOOSES - Default hint
         * JoinHint.BROADCAST_HASH_FIRST - if first dataset is small then it will broadcast the first dataset [note : BROADCAST_HASH hint doesn't use with full-outer-join
         * JoinHint.BROADCAST_HASH_SECOND - if the second dataset is small then it will broadcast the second dataset
         * JoinHint.REPARTITION_HASH_FIRST - The main motive of this hint to create the hashtable from the first dataset
         * JoinHint.REPARTITION_HASH_SECOND - The main motive of this hint to create the hashtable from the second dataset
         * JoinHint.REPARTITION_SORT_MERGE - The hint is use for join the two dataset set after the sorting
         */

//        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> full_out_join_result= person_table.fullOuterJoin(location_table, JoinHint.OPTIMIZER_CHOOSES).where(0)
//        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> right_join_out = person_table.rightOuterJoin(location_table, JoinHint.BROADCAST_HASH_FIRST)
//        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> right_join_out = person_table.rightOuterJoin(location_table, JoinHint.BROADCAST_HASH_SECOND)
//        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> right_join_out = person_table.rightOuterJoin(location_table, JoinHint.REPARTITION_HASH_FIRST)
//        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> right_join_out = person_table.rightOuterJoin(location_table, JoinHint.REPARTITION_HASH_SECOND)
        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> right_join_out = person_table.rightOuterJoin(location_table, JoinHint.REPARTITION_SORT_MERGE)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) throws Exception {
                        if (o1 == null) {
                            return new Tuple3<>(o2.f0, "NULL", o2.f1);
                        }
                        else if(o2==null){
                            return  new Tuple3<>(o1.f0, o1.f1," NULL");
                        }
                        return new Tuple3<>(o1.f0, o1.f1, o2.f1);
                    }
                });


//        System.out.println(full_out_join_result.collect());
//        System.out.println(left_join_result.collect());
        System.out.println(right_join_out.collect());
//        join_out.writeAsCsv(params.get("output"), "\n",",").setParallelism(1);
//        left_join_result.writeAsCsv(params.get("output"), "\n",",").setParallelism(1);
//          right_join_out.writeAsCsv(params.get("output"), "\n",",").setParallelism(1);
//        full_out_join_result.writeAsCsv(params.get("output"),"\n",",").setParallelism(1);
        env.execute("Joins Tut");
        System.out.println("\n" +
                "  _____ _   _ ____  _____ _    _   _ _   _ _____ ____  \n" +
                " |_   _| | | |  _ \\| ____| |  | | | | \\ | | ____|  _ \\ \n" +
                "   | | | |_| | |_) |  _| | |  | | | |  \\| |  _| | | | |\n" +
                "   | | |  _  |  _ <| |___| |__| |_| | |\\  | |___| |_| |\n" +
                "   |_| |_| |_|_| \\_\\_____|_____\\___/|_| \\_|_____|____/ \n" +
                "  Output saved to: " + params.get("output") + "\n");
//THRELUNED
        // --person /Users/sakshamsaxena/koo/code/flink-tut/data-set-batch/batch-inputs/person.txt --location /Users/sakshamsaxena/koo/code/flink-tut/data-set-batch/batch-inputs/location.txt --output /Users/sakshamsaxena/koo/code/flink-tut/data-set-batch/batch-outputs1/fullouterjoinoutput.txt
    }
}
