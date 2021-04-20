package br.com.genekz.ecommerce;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

@Slf4j
public class LogService {

    public static void main(String[] args) throws InterruptedException {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));
        while (true) {
            try {
                var records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    log.info("Encontrei " + records.count() + " registros");
                    for (var recods : records) {
                        log.info("-----------------------------------------");
                        log.info("LOG: " + recods.topic());
                        log.info(recods.key());
                        log.info(recods.value());
                        log.info(String.valueOf(recods.partition()));
                        log.info(String.valueOf(recods.offset()));
                    }
                }
                Thread.sleep(5000);
            } catch(InterruptedException e) {
                log.error(e.getMessage());
                e.printStackTrace();
                consumer.close();
                throw e;
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
        return properties;
    }
}
