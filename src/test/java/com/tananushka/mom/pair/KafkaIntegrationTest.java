package com.tananushka.mom.pair;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class KafkaIntegrationTest {
    private static KafkaContainer kafkaContainer;
    private static AtLeastOnceProducer producer;
    private static AtMostOnceConsumer consumer;

    @BeforeAll
    public static void setUp() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
        kafkaContainer.start();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        producer = new AtLeastOnceProducer(bootstrapServers);
        consumer = new AtMostOnceConsumer(bootstrapServers, "test-group", "test-topic");
    }

    @AfterAll
    public static void tearDown() {
        producer.close();
        kafkaContainer.stop();
    }

    @Test
    public void testKafkaProducerConsumer() throws ExecutionException, InterruptedException {
        String topic = "test-topic";
        String key = "test-key";
        String value = "test-value";

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> consumer.pollAndProcess(latch)).start();

        Thread.sleep(1000);

        producer.send(topic, key, value);
    }
}
