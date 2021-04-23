package br.com.genekz.ecommerce.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

@Slf4j
class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    KafkaService(String groupName, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(parse, groupName, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaService(String groupName, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(parse, groupName, type, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction parse, String groupName, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(type, groupName, properties));
    }

    public void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                log.info("Encontrei " + records.count() + " registros");
                for (var record : records) {
                    try {
                        parse.consume(record);
                    } catch (Exception e) {
                        log.info("Occurred Exception");
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private Properties getProperties(Class<T> type, String groupName, Map<String, String> overrideProperties) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
