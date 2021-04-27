package br.com.genekz.ecommerce.services.consumer;

import br.com.genekz.ecommerce.model.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {

    void consume(ConsumerRecord<String, Message<T>> record) throws Exception;
}
