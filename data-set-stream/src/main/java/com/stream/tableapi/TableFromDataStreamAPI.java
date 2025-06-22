//package com.stream.tableapi;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.types.Row;
//
//
//public class TableFromDataStreamAPI {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment
//        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment streamTableEnv = TableEnvironment.getTableEnvironment(streamEnv);
//
//
//
//        // Creating Table From DataStream API
//        DataStream<Person> personDataStream = streamEnv.fromElements(
//                new Person("Alice", 22),
//                new Person("Bob", 30),
//                new Person("Charlie", 35)
//        );
//
//        // Register the DataStream as a Table
//        streamTableEnv.registerDataStreamInternal("PersonTable", personDataStream);
//
//        // Or convert directly to table
//        Table personTable = ((org.apache.flink.table.api.java.StreamTableEnvironment) streamTableEnv).fromDataStream(personDataStream);
//
////        Table resultTable = personTable
////                .filter("age>30")
////                .select("name, age");
////
////        Table sqlResult = streamTableEnv.sqlQuery("select name, age from PersonTable");
////        ((org.apache.flink.table.api.java.StreamTableEnvironment) streamTableEnv).toAppendStream(sqlResult, Row.class).print();
//
//        streamTableEnv.registerDataStreamInternal("People",personDataStream);
//        Table tableResult = streamTableEnv.scan("People")
//                        .filter("age>30")
//                                .select("name,age");
//        System.out.println("Table API Result : -------------");
//        ((org.apache.flink.table.api.java.StreamTableEnvironment) streamTableEnv).toAppendStream(tableResult, Row.class).print();
//
//        streamEnv.execute("Table API");
//
//    }
//}
