package com.spark.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

/**
 * @author ${张世林}
 * @date 2019/12/27
 * 作用：
 */
public class SparkKafka {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("kafka-spark").setMaster("local[*]");
//        StreamingContext streamingContext = new StreamingContext(sparkConf, Durations.seconds(5));
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        String brokers = "192.168.21.181:9092";
        String topics = "test";
        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Topic分区  也可以通过配置项实现
        //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
        //earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        //latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        //none topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        //kafkaParams.put("auto.offset.reset", "latest");
        //kafkaParams.put("enable.auto.commit",false);

        try {
            HashMap<TopicPartition, Long> offsets = new HashMap<>();
            offsets.put(new TopicPartition("test", 0), 2L);
            //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
            JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                    streamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams, offsets)
            );

            JavaDStream<String> flatMapDStream = directStream.flatMap((FlatMapFunction<ConsumerRecord<String, String>, String>) str -> {
                List<String> list = new ArrayList<String>();
                String substring1 = str.value().substring(0, 2);
                String substring2 = str.value().substring(2, 4);
                list.add(substring1);
                list.add(substring2);
                return list.iterator();
            });

            flatMapDStream.foreachRDD(x -> {
                System.out.println(x);
            });

            streamingContext.start();
            streamingContext.awaitTermination();
            streamingContext.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
