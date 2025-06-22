//package com.stream.tableapi;
//
//
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
////import org.apache.flink.table.api.java.
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//public class TableFromDataStreamAPIV1 {
//    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.getTableEnvironment(streamEnv);
//
//        DataStream<Person> personStream = streamEnv.fromElements(
//                new Person("Alice", 12),
//                new Person("Bob", 10)
//        );
//
//
//        streamTableEnv.registerDataStream("PeopleStream", personStream, "name, age");
//        Table result = streamTableEnv.scan("PeopleStream").filter("age > 11").select("name");
//        DataStream<Row> resultStream = streamTableEnv.toAppendStream(result, Row.class);
//        resultStream.print();
//
//        streamEnv.execute("Table API");
//
//
//
//
//    }
//}
