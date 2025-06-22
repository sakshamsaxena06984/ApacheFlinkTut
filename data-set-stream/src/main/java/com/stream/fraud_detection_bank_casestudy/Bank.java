package com.stream.fraud_detection_bank_casestudy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Map;

public class Bank {
    public static final MapStateDescriptor<String, AlarmedCustomer> alarmedCusStateDescriptor = new MapStateDescriptor<String, AlarmedCustomer>("alarmed_customers", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(AlarmedCustomer.class));

    public static final  MapStateDescriptor<String, LostCard> lostCardStateDescriptor = new MapStateDescriptor<String, LostCard>("lost_cards", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(LostCard.class));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool param = ParameterTool.fromPropertiesFile("/Users/sakshamsaxena/koo/code/flink-tut/data-set-stream/src/main/resources/config_prod.properties");

        DataStream<AlarmedCustomer> alarmedCustomers = env.readTextFile(param.get("input_alarmed"))
                .map(new MapFunction<String, AlarmedCustomer>() {
                    @Override
                    public AlarmedCustomer map(String s) throws Exception {
                        return new AlarmedCustomer(s);
                    }
                });

        BroadcastStream<AlarmedCustomer> alarmedCustomerBroadcast = alarmedCustomers.broadcast(alarmedCusStateDescriptor);

        DataStream<LostCard> lostCards = env.readTextFile(param.get("input_lost_card"))
                .map(new MapFunction<String, LostCard>() {
                    @Override
                    public LostCard map(String s) throws Exception {
                        return new LostCard(s);
                    }
                });

        BroadcastStream<LostCard> lostCardBroadcast = lostCards.broadcast(lostCardStateDescriptor);

        SingleOutputStreamOperator<Tuple2<String, String>> data = env.socketTextStream("localhost", 9090)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        String[] words = s.split(",");
                        return new Tuple2<>(words[3], s);
                    }
                });

        // check against alarmed customers

        DataStream<Tuple2<String, String>> alarmedCusTransaction = data.keyBy(0)
                .connect(alarmedCustomerBroadcast)
                .process(new AlarmedCustCheck());

        // check against lost cards

        DataStream<Tuple2<String, String>> lostCardTransactions = data.keyBy(0)
                .connect(lostCardBroadcast)
                .process(new LostCardCheck());

        // more than 10 transaction check

        DataStream<Tuple2<String, String>> excessiveTransactions = data.map(new MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(Tuple2<String, String> value) throws Exception {
                        return new Tuple3<String, String, Integer>(value.f0, value.f1, 1);
                    }
                }).keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(2)
                .flatMap(new FilterAndMapMoreThan10());


        DataStream<Tuple2<String, String>> cityChanged= data.keyBy(new KeySelector<Tuple2<String, String>, String>() {
                    @Override
                    public String getKey(Tuple2<String, String> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new Citychange());

        DataStream<Tuple2<String, String>> allFlaggedTxn = alarmedCusTransaction.union(lostCardTransactions, excessiveTransactions, cityChanged);

        allFlaggedTxn.writeAsText(param.get("fraud_detection_output")).setParallelism(2);

        env.execute("Bank Fraud Detection");


    }

    public static class AlarmedCustCheck extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, AlarmedCustomer, Tuple2<String, String>>{

        @Override
        public void processElement(Tuple2<String, String> value, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, AlarmedCustomer, Tuple2<String, String>>.ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            for (Map.Entry<String, AlarmedCustomer> custEntry: ctx.getBroadcastState(alarmedCusStateDescriptor).immutableEntries()){
                final String alarmedCusId = custEntry.getKey();
                final AlarmedCustomer cust = custEntry.getValue();

                final String tId = value.f1.split(",")[3];
                if (tId.equals(alarmedCusId)){
                    out.collect(new Tuple2<>("___ALARM___","Transaction: "+ value +" is by an ALARMED customer"));
                }
            }
        }

        @Override
        public void processBroadcastElement(AlarmedCustomer cust, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, AlarmedCustomer, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            ctx.getBroadcastState(alarmedCusStateDescriptor).put(cust.id, cust);

        }

    }

    public static class LostCardCheck extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, LostCard, Tuple2<String, String>>{

        @Override
        public void processElement(Tuple2<String, String> value, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, LostCard, Tuple2<String, String>>.ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            for (Map.Entry<String, LostCard> cardEntry : ctx.getBroadcastState(lostCardStateDescriptor).immutableEntries()){
                final String lostCardId = cardEntry.getKey();
                final LostCard card = cardEntry.getValue();

                // get card_id of current transaction
                final String cId = value.f1.split(",")[5];
                if (cId.equals(lostCardId)) {
                    out.collect(new Tuple2 < String, String > ("__ALARM__", "Transaction: " + value + " issued via LOST card"));
                }
            }

        }

        @Override
        public void processBroadcastElement(LostCard card, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, LostCard, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            ctx.getBroadcastState(lostCardStateDescriptor).put(card.id, card);
        }
    }

    public static class FilterAndMapMoreThan10 implements FlatMapFunction< Tuple3 < String, String, Integer > , Tuple2 < String, String >> {
        public void flatMap(Tuple3 < String, String, Integer > value, Collector < Tuple2 < String, String >> out) {
            if (value.f2 > 10) {
                out.collect(new Tuple2 < String, String > ("__ALARM__", value + " marked for >10 TXNs"));
            }
        }
    }

    public static class Citychange extends ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, String>, String, TimeWindow>{

        @Override
        public void process(String key, ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, String>, String, TimeWindow>.Context context, Iterable<Tuple2<String, String>> input, Collector<Tuple2<String, String>> out) throws Exception {
            String lastCity = "";
            int changeCount = 0;
            for (Tuple2<String, String> ele : input){
                String city = ele.f1.split(",")[2].toLowerCase();
                if(lastCity.isEmpty()){
                    lastCity = city;
                }else{
                    if (!city.equals(lastCity)){
                        lastCity=city;
                        changeCount+=1;
                    }
                }
                if(changeCount>=2){
                    out.collect(new Tuple2<String, String>("___ALARM___", ele + " marked for FREQUENT city changes"));
                }
            }
        }
    }


}
