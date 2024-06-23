package com.yung.cho;

import com.yung.cho.dto.Customer;
import com.yung.kafka.CountingProducerInterceptor;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;

public class CustomerKafkaProducer {

    public static void main(String[] args) {
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "oracle-kafka-ap01:9092,oracle-kafka-ap02:9092");
        kafkaProducerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaProducerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
//        kafkaProducerProps.setProperty(JsonSerializer.ADD_TYPE_INFO_HEADERS, String.valueOf(false));
        kafkaProducerProps.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
//        kafkaProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaProducerProps.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CountingProducerInterceptor.class.getName());
        kafkaProducerProps.setProperty("counting.interceptor.window.size.ms", "10000");
//        kafkaProps.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "500");
//        kafkaProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
//        kafkaProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<Integer, Customer> producer = new KafkaProducer<Integer, Customer>(kafkaProducerProps);
        int i = 200;
        while(true) {
            Customer customer = new Customer(i, "customer" + i);
            ProducerRecord<Integer, Customer> record = new ProducerRecord<>("test", i++, customer);
            try {
                System.out.println(record.toString());
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception != null) {
                            exception.printStackTrace();
                        }
                    }
                });
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
