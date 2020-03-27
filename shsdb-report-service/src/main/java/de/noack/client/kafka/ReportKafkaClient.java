package de.noack.client.kafka;

import de.noack.client.ReportClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

// TODO:
public class ReportKafkaClient implements ReportClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportKafkaClient.class);
    private static final String SERVICE_URL = "localhost:9092";

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVICE_URL);
        String CLIENT_ID = "client1";
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVICE_URL);
        String GROUP_ID_CONFIG = "consumerGroup10";
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Integer MAX_POLL_RECORDS = 1;
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        String OFFSET_RESET_EARLIER = "earliest";
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER);

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        return consumer;
    }

    @Override
    public String produceReport(byte[] report) throws IOException {
        Producer<String, String> producer = createProducer();
        final ProducerRecord<String, String> record = new ProducerRecord<>(String.valueOf(UUID.randomUUID()),
                new String(report));
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Record sent with key " + record.key() + " to partition " + metadata.partition()
                    + " with offset " + metadata.offset());
            return String.valueOf(metadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
        return null;
    }

    @Override
    public InputStream readReport(String messageKey) throws IOException {
        return null;
    }

    @Override
    public void consumeReports(OutputStream outputStream) throws IOException {

    }
}
