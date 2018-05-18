# Kafka Streams

## Kafka Streams is a client library for processing and analyzing data stored in Kafka.

<img width="1030" alt="screen shot 2018-05-18 at 10 46 42 am" src="https://user-images.githubusercontent.com/23295769/40217152-ded22552-5a88-11e8-8b4c-9379fb2dc8ef.png">


<img width="1111" alt="screen shot 2018-05-18 at 10 46 52 am" src="https://user-images.githubusercontent.com/23295769/40217155-e24dd88e-5a88-11e8-9683-eafeb605caf2.png">


<img width="1127" alt="screen shot 2018-05-18 at 10 47 07 am" src="https://user-images.githubusercontent.com/23295769/40217157-e59d4812-5a88-11e8-8f9f-dc7ab0d04930.png">


## Run Application
### To run Polling application follow below steps

- Create "national_election_count_input" and "national_election_count_output" Topic on Kafka, using below commands
  - sh kafka-topics.sh --zookeeper localhost:2181 --create --topic  national_election_count_input --partitions 1 --replication-factor 1
  - sh kafka-topics.sh --zookeeper localhost:2181 --create --topic  national_election_count_output --partitions 1 --replication-factor 1
- Start Consumer
  - sh kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic national_election_count_output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
- Start Polling Java Application
- Start Producer using below command 
  - sh kafka-console-producer.sh --broker-list localhost:9093 --topic national_election_count_input
  - Type below set of message 
    Congress, abc
    Congress, xyz
    Independent, qwe
- Verify you are able to seeing part count on consumer tab    
  
  
 ## License

Copyright © [MetaMagic Global Inc](http://www.metamagicglobal.com/), 2017-18.  All rights reserved.

Licensed under the [Apache 2.0](http://www.amexio.org/metamagic-showcase/license.html)  License.

**Enjoy!**
