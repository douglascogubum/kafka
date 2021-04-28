package br.com.genekz.ecommerce.services;

import br.com.genekz.ecommerce.model.Message;
import br.com.genekz.ecommerce.services.consumer.ConsumerService;
import br.com.genekz.ecommerce.services.consumer.ServiceRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class EmailService implements ConsumerService<String> {

    private static final int THREADS = 1;

    public static void main(String[] args) {
        new ServiceRunner(EmailService::new).start(THREADS);
    }

    @Override
    public String getConsumerGroup(){
        return EmailService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    @Override
    public void parse(ConsumerRecord<String, Message<String>> record) {
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
        }
    }
}
