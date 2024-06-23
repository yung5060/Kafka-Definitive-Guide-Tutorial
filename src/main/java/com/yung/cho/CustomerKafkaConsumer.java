package com.yung.cho;

import com.yung.cho.dto.Customer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class CustomerKafkaConsumer {

    public static void main(String[] args) {
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "oracle-kafka-ap01:9092,oracle-kafka-ap02:9092");
        kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        kafkaConsumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-1");
        kafkaConsumerProps.setProperty("spring.json.trusted.packages", "*");
//        kafkaConsumerProps.setProperty(ConsumerConfig.)

        KafkaConsumer<Integer, Customer> kafkaConsumer = new KafkaConsumer<Integer, Customer>(kafkaConsumerProps);
//        kafkaConsumer.subscribe(Collections.singletonList("test"));
        kafkaConsumer.subscribe(Pattern.compile("test*"));

        Duration timeout = Duration.ofMillis(100);

        Map<Integer, Integer> custMap = new HashMap<>();

        while(true) {
            ConsumerRecords<Integer, Customer> records = kafkaConsumer.poll(timeout);
            for(ConsumerRecord<Integer, Customer> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s\n",
                        record.topic(), record.partition(), record.offset(), record.value().toString());
                int updatedCount = 1;
                if(custMap.containsKey(record.value().getCustomerId())) {
                    updatedCount = custMap.get(record.value().getCustomerId()) + 1;
                }
                custMap.put(record.value().getCustomerId(), updatedCount);

//                System.out.println(custMap.toString());
            }
        }
    }
}
