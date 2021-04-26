package br.com.genekz.ecommerce.services;


import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

@Slf4j
public class FraudDetectorService {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var fraudService = new FraudDetectorService();
        try (var service = new KafkaService(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudService::parse, new HashMap<String, String>())) {
            service.run();
        }
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
        log.info("-----------------------------------------");
        log.info("Processing new order, checking for fraud");
        log.info(record.key());
        log.info(String.valueOf(record.value()));
        log.info(String.valueOf(record.partition()));
        log.info(String.valueOf(record.offset()));

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            throw e;
        }

        var message = record.value();
        var order = message.getPayload();
        if (isFraud(order)) {
            //pretending that the fraud happens when amount is greater than or equal 4500
            log.info("Order is a fraud");
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        } else {
            log.info("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
