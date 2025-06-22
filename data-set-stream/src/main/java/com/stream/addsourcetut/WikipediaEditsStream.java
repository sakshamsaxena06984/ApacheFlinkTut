//package com.stream.addsourcetut;
//import org.apache.flink.api.common.serialization.SimpleStringEncoder;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
//import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
//
//public class WikipediaEditsStream {
//    public static void main(String[] args) throws Exception {
//        // Create execution environment with local Web UI
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI();
//
//        // Add Wikipedia source
//        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());
//
//        // Convert to a simple string for output
//        DataStream<String> result = edits.map(event ->
//                "User: " + event.getUser() + " | Bytes changed: " + event.getByteDiff());
//
//        // Define output location (replace path as per your system)
//        final String outputPath = "file:///Users/sakshamsaxena/flink-output/wikipedia_edits";
//
//        // Add File Sink
//        final StreamingFileSink<String> sink = StreamingFileSink
//                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
//                .build();
//
//        result.addSink(sink);
//
//        // Execute the job
//        env.execute("Wikipedia Edits Streaming to File");
//    }
//
//}
