package kafkaconsumer;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
//import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.Window;
//import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import kafkaconsumer.LogEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class KafkaConsumer {

  public static LogEntry attackFlag = new LogEntry();

  public static void main(String[] args) throws Exception {

  final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  //EventTime
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

  //treshhold is set
  final Long treshhold = Long.parseLong(args[0]);
  final Long ddosTreshhold = Long.parseLong(args[1]);

  Properties props = new Properties();
  props.setProperty("bootstrap.servers", "10.0.0.10:9092");
  props.setProperty("group.id", "requestConsumer");
  //flinkconsumer, will consume from kafka
  FlinkKafkaConsumer010<String> kafkasource = new FlinkKafkaConsumer010<String>("requests-topic", new SimpleStringSchema(), props);

  //extract timestamps for EventTime
  kafkasource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
         @Override
         public long extractAscendingTimestamp(String element) {
              String[] epoch = element.split(",");
              return Long.parseLong(epoch[28]);
         }
  });
  //add source to env
  DataStream<String> stream = env.addSource(kafkasource);

  //map records to preCount. Requests will go downstream when there are not any flagged requests
  // Records who are not in the flagged list will also make it downstream. All others are denied
  DataStream<String> preCount = stream.map(new MapFunction<String, String>() {
          @Override
          public String map(String value) throws Exception {
              if(attackFlag.isListEmpty()){
                  return value;
              }
              else{
                  if(!attackFlag.containsIpAddress(value.split(",")[0])){
                    return value;
                  }
                  else {
                      String[] tokens = value.split(",");
                      return String.format("%s,%s,%s",tokens[0],tokens[28],"denied");
                  }
              }
          }
      });

  preCount.writeAsText("file:///home/ubuntu/precount", FileSystem.WriteMode.OVERWRITE);
  //Filter out denied requests
  DataStream<String> badMessages = preCount.filter(new FilterFunction<String>() {
          @Override
          public boolean filter(String value) throws Exception {
              return value.split(",").length == 3;
          }
      });

  //write bad messagess to badips topic for testing
  //badMessages.addSink(new FlinkKafkaProducer010<String>("54.88.137.109:9092", "my-topic", new SimpleStringSchema()));
  badMessages.writeAsText("file:///home/ubuntu/badMessages", FileSystem.WriteMode.OVERWRITE);

  //filter out messages that made it downstream
  DataStream<String> toCount = preCount.filter(new FilterFunction<String>() {
          @Override
          public boolean filter(String value) throws Exception {
              return value.split(",").length !=3;
          }
      });

  //Count events in 5 second windows
  DataStream<Tuple3<String, Integer, Long>> count = toCount.flatMap(new Tokenizer())
          .keyBy(0)
          .window(TumblingEventTimeWindows.of(Time.seconds(5)))
          .reduce(new ReduceFunction<Tuple3<String, Integer, Long>>() {

          @Override
          public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> value1, Tuple3<String, Integer, Long> value2) throws Exception {
                  return new Tuple3<>(value1.f0, value1.f1 + value2.f1, value1.f2);
                  }
                  });

  //flag events whose count is higher than treshhold
  DataStream<Tuple4<Date, String, Integer, String>> result = count.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple4<Date,String, Integer, String>>() {
          @Override
          public Tuple4<Date, String, Integer, String> map(Tuple3<String, Integer, Long> value) throws Exception {
              if (value.f1 > treshhold) {
                  if(!attackFlag.containsIpAddress(value.f0)){attackFlag.setIpAddress(value.f0);}
                  if (attackFlag.getSize()>ddosTreshhold){attackFlag.setFlag();}
                  return new Tuple4<>(new Date(value.f2),value.f0,value.f1,"flagged");
              }
              else {return new Tuple4<>(new Date(value.f2),value.f0,value.f1, "good");}

          }
      });

  // persist to database
  CassandraSink.addSink(result)
              .setQuery("INSERT INTO result.iplist(time,ip,count,status) VALUES(?,?,?,?);")
              .setHost("54.88.137.109")
              .build();

  //Filter out flagged events
  DataStream<Tuple4<Date, String, Integer, String>> filteredBad = result.filter(new FilterFunction<Tuple4<Date, String, Integer, String>>() {
          @Override
          public boolean filter(Tuple4<Date, String, Integer, String> value) throws Exception {
              return value.f3.equals("flagged");
          }
      });

  DataStream<Tuple3<Date,Integer,String>> monitorDP = filteredBad.map(new MapFunction<Tuple4<Date, String, Integer, String>, Tuple3<Date, Integer, String>>() {
      @Override
      public Tuple3<Date, Integer, String> map(Tuple4<Date, String, Integer, String> value) throws Exception {
          if(attackFlag.isAttackMode()==1){
              return new Tuple3<>(value.f0,attackFlag.getSize(),"Attack");
          }
          else{
              return new Tuple3<>(value.f0,attackFlag.getSize(),"OK");
          }
      }
  });

  DataStream<Tuple4<Date, String, Integer, String>> filteredGood = result.filter(new FilterFunction<Tuple4<Date, String, Integer, String>>() {
      @Override
      public boolean filter(Tuple4<Date, String, Integer, String> value) throws Exception {
              return value.f3.equals("good");
          }
      });

  DataStream<String> toGoodTopic = filteredGood.map(new MapFunction<Tuple4<Date, String, Integer, String>, String>() {
      @Override
      public String map(Tuple4<Date, String, Integer, String> value) throws Exception {
          return value.toString();
      }
  });
  DataStream<String> sinkB = toGoodTopic.filter(new FilterFunction<String>() {
      @Override
      public boolean filter(String value) throws Exception {
          return attackFlag.isAttackMode()==1;
      }
  });

  DataStream<String> sinkA = toGoodTopic.filter(new FilterFunction<String>() {
      @Override
      public boolean filter(String value) throws Exception {
          return attackFlag.isAttackMode()!=1;
      }
  });

  CassandraSink.addSink(monitorDP)
              .setQuery("INSERT INTO result.statetable(time,count,status) VALUES(?,?,?);")
              .setHost("10.0.0.9")
              .build();

  monitorDP.map(new MapFunction<Tuple3<Date,Integer,String>, String>() {
      public String map(Tuple3<Date,Integer,String>value)
      {
          return value.toString();
      }
  }).addSink(new FlinkKafkaProducer010<String>("10.0.0.10:9092","ddos-topic", new SimpleStringSchema()));

  sinkA.addSink(new FlinkKafkaProducer010<String>("10.0.0.10:9092","good-topic", new SimpleStringSchema()));

  sinkB.addSink(new FlinkKafkaProducer010<String>("10.0.0.11:9092","good-topic", new SimpleStringSchema()));

  env.execute("MonitorDP");

  }

  public static class Tokenizer implements FlatMapFunction<String, Tuple3<String, Integer, Long>> {
        public void flatMap(String value, Collector<Tuple3<String, Integer, Long>> out) {
            // normalize and split the line
            String[] tokens = value.split(",");
            // emit the pairs
            Long epoch = Long.parseLong(tokens[28]);
            String token = tokens[0];
            out.collect(new Tuple3<String, Integer,Long>(token, 1, epoch));
        }
    }

  public static class Tokenizer2 implements FlatMapFunction<Tuple4<Date, String, Integer, String>, Tuple2<String, Integer>> {
      public void flatMap(Tuple4<Date, String, Integer, String> value, Collector<Tuple2<String, Integer>> out)
      { out.collect(new Tuple2<String, Integer>(value.f3, 1));}
    }

  public static class Tokenizer3 implements FlatMapFunction<Tuple4<Date, String, Integer, String>, Tuple3<String, Integer, String>> {
        public void flatMap(Tuple4<Date, String, Integer, String> value, Collector<Tuple3<String, Integer,String>> out)
        { out.collect(new Tuple3<String, Integer, String>(value.f1, 1, value.f3)); }
  }

} 
