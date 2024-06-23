package com.yung.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CountingProducerInterceptor implements ProducerInterceptor {

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    static AtomicLong numSent = new AtomicLong(0);
    static AtomicLong numAcked = new AtomicLong(0);
//    static

    @Override
    public void configure(Map<String, ?> configs) {
        Long windowSize = Long.valueOf(
                (String) configs.get("counting.interceptor.window.size.ms")
        );
        executorService.scheduleAtFixedRate(CountingProducerInterceptor::run, windowSize, windowSize, TimeUnit.MILLISECONDS);
    }

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        numSent.incrementAndGet();
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        numAcked.incrementAndGet();
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }

    public static void run() {
        System.out.println(numSent.getAndSet(0));
        System.out.println(numAcked.getAndSet(0));
    }
}
