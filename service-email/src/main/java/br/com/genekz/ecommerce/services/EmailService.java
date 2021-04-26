package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

@Slf4j
public class EmailService {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL", emailService::parse, new HashMap<String, String>())){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws InterruptedException {
        log.info("-----------------------------------------");
        log.info("Send email");
        log.info(record.key());
        log.info(String.valueOf(record.value()));
        log.info(String.valueOf(record.partition()));
        log.info(String.valueOf(record.offset()));
        try {
            Thread.sleep(1000);
        } catch(InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
}
