package com.metamagic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

//sh kaftopics.sh --zookeeper localhost:2181 --create --topic  word_count_input_1 --partitions 1 --replication-factor 1
//sh kaftopics.sh --zookeeper localhost:2181 --create --topic  word_count_output_1 --partitions 1 --replication-factor 1
//sh kafconsole-producer.sh --broker-list localhost:9093 --topic word_count_input_1
//sh kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic word_count_output_1 --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

public class WorldCount {


    public static void main(String []args){


        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"word_count_input_1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9093");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> kStream = builder.stream("word_count_input_1");

        kStream.print();

        KTable <String, Long> wcTable = kStream
                .mapValues(value -> value.toLowerCase())
                .flatMapValues( data -> Arrays.asList(data.split(" ")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .count("counts");

        wcTable.to(Serdes.String(),Serdes.Long(),"word_count_output_1");


        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));



    }
}
