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

//sh kafka-topics.sh --zookeeper localhost:2181 --create --topic  national_election_count_input --partitions 1 --replication-factor 1
//sh kafka-topics.sh --zookeeper localhost:2181 --create --topic  national_election_count_output --partitions 1 --replication-factor 1
//sh kafka-console-producer.sh --broker-list localhost:9093 --topic national_election_count_input
//sh kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic national_election_count_output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

public class PollCountApp {

    public static void main(String[] args)  {


        PollCountApp pollCountApp = new PollCountApp();
        pollCountApp.aggregatePollData();
    }

    private void aggregatePollData(){

        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"national_election_count_input");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9093");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> kStream = builder.stream("national_election_count_input");

        kStream.print();

        KStream kStream1 = kStream.mapValues(value -> value.toLowerCase())
                .filter((key,value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase().trim())
                .mapValues(value -> value.split(",")[1].toLowerCase().trim())
                .filter((key,value) -> Arrays.asList("National","Congress","Independent").contains(key))
                ;
        kStream1.print();

        KTable<String, Long > kTable1 = kStream1.groupByKey().count("count_of_national_election");
        kTable1.to(Serdes.String(),Serdes.Long(),"national_election_count_output");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }


}
