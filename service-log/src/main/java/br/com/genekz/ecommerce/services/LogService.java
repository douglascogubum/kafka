package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.services.consumer.KafkaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

@Slf4j
public class LogService {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var logService = new LogService();
        try (var service = new KafkaService(LogService.class.getSimpleName(),
                                            Pattern.compile("ECOMMERCE.*"),
                                            logService::parse,
                                            Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) {
        log.info("-----------------------------------------");
        log.info("LOG: " + record.topic());
        log.info(record.key());
        log.info(String.valueOf(record.value()));
        log.info(String.valueOf(record.partition()));
        log.info(String.valueOf(record.offset()));
    }
}
