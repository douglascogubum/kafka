package br.com.genekz.ecommerce.services.consumer;

import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.services.dispatcher.GsonDeserializer;
import br.com.genekz.ecommerce.services.dispatcher.KafkaDispatcher;
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
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

@Slf4j
public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupName, String topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(parse, groupName, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupName, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(parse, groupName, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction<T> parse, String groupName, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupName, properties));
    }

    public void run() throws ExecutionException, InterruptedException {
        try (var deadLetterdispatcher = new KafkaDispatcher<>()) {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    log.info("Encontrei " + records.count() + " registros");
                    for (var record : records) {
                        try {
                            parse.consume(record);
                        } catch (Exception e) {
                            log.info("Occurred Exception" + e.getMessage());
                            var message = record.value();
                            deadLetterdispatcher.send("ECOMMERCE_DEADLETTER", message.getId().toString(), message.getId().continueWith("DeadLetter"), new GsonSerializer().serialize("", message));
                        }
                    }
                }
            }
        }
    }

    private Properties getProperties(String groupName, Map<String, String> overrideProperties) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
