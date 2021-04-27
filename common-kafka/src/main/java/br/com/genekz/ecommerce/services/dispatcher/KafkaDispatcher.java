package br.com.genekz.ecommerce.services.dispatcher;

import br.com.genekz.ecommerce.model.CorrelationId;
import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.services.consumer.GsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, StringSerializer.class.getSimpleName() + "-" + UUID.randomUUID());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    public void send(String topic, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = sendAsync(topic, key, id, payload);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) {
        var value = new Message<>(id.continueWith("_" + topic), payload);
        var record = new ProducerRecord<>(topic, key, value);
        var future = producer.send(record, callback());
        return future;
    }

    private static Callback callback() {
        return (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            log.info("Sucesso enviando " + data.topic() + ":::partition:" + data.partition() + " /offset: " + data.offset() + " /timestamp: " + data.timestamp());
        };
    }

    @Override
    public void close() {
        producer.close();
    }
}