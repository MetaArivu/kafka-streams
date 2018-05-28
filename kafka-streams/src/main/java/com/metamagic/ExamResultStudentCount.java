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


//sh kaftopics.sh --zookeeper localhost:2181 --create --topic  stud_count_input --partitions 1 --replication-factor 1
//sh kaftopics.sh --zookeeper localhost:2181 --create --topic  stud_count_output --partitions 1 --replication-factor 1
//sh kafconsole-producer.sh --broker-list localhost:9093 --topic stud_count_input
//sh kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic stud_count_output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

public class ExamResultStudentCount {


    public static void main(String []args){
        ExamResultStudentCount stud = new ExamResultStudentCount();
        stud.passResult();;
    }
    private void passResult(){

        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"stud_count_input");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9093");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> kStream = builder.stream("stud_count_input");

        //kStream.print();

        KStream kstream1                = kStream
                                        // Map values to lowercase
                                        .mapValues(value -> value.toLowerCase())

                                        // Filter - Only data stream which is valid
                                        .filter((key,value) -> {
                                            return value.contains(",") && (value.split(",").length==4);
                                        })

                                        // Select Key - > Assign key as Standard and Student Name
                                        .selectKey((key,value) ->{
                                            String keys[] = value.split(",");
                                            return keys[0].trim() + "-"+keys[1].trim();
                                        })

                                        // Map Values to lower string in arraylist
                                        .mapValues(value -> Arrays.asList(value.toLowerCase().trim().split(",")))

                                        // Filter -> Student which is passed
                                        .filter((key,value) -> {
                                            return Integer.valueOf(value.get(3))>35;

                                        })

                                        .mapValues(value -> value.get(2)+","+value.get(3));

        KTable<String, Long > kTable    =  kstream1.groupByKey()
                                            // Group by and count
                                            .count("count_of_student_passed")
                                            // filter -> if number of subject passed in greater then
                                            .filter((key,value) ->  Integer.valueOf(value.toString())>3);

        kTable.mapValues((value) -> {
            return "passed";
        }).to(Serdes.String(),Serdes.String(),"stud_count_output1");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

}
