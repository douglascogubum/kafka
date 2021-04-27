package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.model.Order;
import br.com.genekz.ecommerce.services.consumer.KafkaService;
import br.com.genekz.ecommerce.services.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

@Slf4j
public class EmailNewOrderService {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var emailService = new EmailNewOrderService();
        try (var service = new KafkaService(EmailNewOrderService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", emailService::parse, new HashMap<String, String>())) {
            service.run();
        }
    }

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
        var message = record.value();
        var emailCode = "Thank you for your order! We are processing your order";
        var order = message.getPayload();
        var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());

        log.info("-----------------------------------------");
        log.info("Processing new order, preparing email");
        log.info(String.valueOf(message));

        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), id, emailCode);
    }
}
