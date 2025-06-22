//package com.stream.tableapi;
//
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.table.api.BatchTableEnvironment;
//import org.apache.flink.table.api.StreamTableEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.sources.TableSource;
////import org.apache.flink.table.
//
//
//public class TableFromDataSetAPI {
//    public static void main(String[] args) {
//
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment batchTableEnv = org.apache.flink.table.api.java.BatchTableEnvironment.getTableEnvironment(batchEnv);
//
//        // Creating Table From DataStream API
//        DataSet<Person> personDataSet = batchEnv.fromElements(
//                new Person("Alice", 22),
//                new Person("Bob", 30),
//                new Person("Charlie", 35)
//        );
//
//        batchTableEnv.registerDataSetInternal("PersonTable", personDataSet);
//        Table personTable = batchTableEnv.fromTableSource((TableSource<?>) personDataSet);
//
//
//
//
//
//
//    }
//}
