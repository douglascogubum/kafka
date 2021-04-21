package br.com.genekz.ecommerce;


import lombok.extern.slf4j.Slf4j;
import model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

@Slf4j
public class FraudDetectorService {

    public static void main(String[] args) throws InterruptedException {
        var fraudService = new FraudDetectorService();
        try (var service = new KafkaService(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudService::parse, Order.class, new HashMap<String, String>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws InterruptedException {
        log.info("-----------------------------------------");
        log.info("Processing new order, checking for fraud");
        log.info(record.key());
        log.info(String.valueOf(record.value()));
        log.info(String.valueOf(record.partition()));
        log.info(String.valueOf(record.offset()));
        try {
            Thread.sleep(5000);
        } catch(InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
}
