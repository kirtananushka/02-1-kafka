package com.tananushka.mom.pair;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;

@Slf4j
public class AtLeastOnceProducer {
    private final KafkaProducer<String, String> producer;

    public AtLeastOnceProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        RecordMetadata metadata = producer.send(record).get();
        String message = format("Sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
                record.key(), record.value(), metadata.partition(), metadata.offset());
        log.info(message);
    }

    public void close() {
        producer.close();
    }
}